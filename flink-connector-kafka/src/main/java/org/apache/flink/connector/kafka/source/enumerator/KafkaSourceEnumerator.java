/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * KafkaSourceEnumerator 负责管理 KafkaSource 任务的分区（Partition）分配，
 * 以及动态分区发现（Partition Discovery）。
 *
 * <p>在 Flink 的 KafkaSource 连接器中，每个 `KafkaSourceEnumerator` 运行在
 * JobManager 上，负责：
 * 1. 初始化 Kafka 主题的分区信息（Partition Discovery）。
 * 2. 将 Kafka 分区（TopicPartition）分配给 Flink 任务（SourceReader）。
 * 3. 监听 Kafka 主题的变化，并在新分区出现时动态分配。
 * 4. 维护分区的 Offset 位置，并在 Flink Checkpoint 期间存储状态。
 *
 * <p>KafkaSourceEnumerator 实现了 `SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState>`，
 * 其中：
 * - `KafkaPartitionSplit` 代表 Kafka 主题的分片（Partition）。
 * - `KafkaSourceEnumState` 代表枚举器的状态（包含已分配/未分配的分区信息）。
 */
@Internal
public class KafkaSourceEnumerator
        implements SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> {

    /** Logger 日志对象 */
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceEnumerator.class);

    /** 订阅 Kafka 主题的策略（订阅单个主题/多个主题/特定分区） */
    private final KafkaSubscriber subscriber;

    /** Kafka 读取起始偏移量的策略（如 earliest, latest, timestamp, specific offset） */
    private final OffsetsInitializer startingOffsetInitializer;

    /** Kafka 读取停止偏移量的策略（用于有限流） */
    private final OffsetsInitializer stoppingOffsetInitializer;

    /** 发现新分区时，使用的 Offset 初始化策略（通常为 earliest） */
    private final OffsetsInitializer newDiscoveryOffsetsInitializer;

    /** Kafka Consumer 相关的配置参数（如 broker 地址, group.id 等） */
    private final Properties properties;

    /** Kafka 分区发现的时间间隔（毫秒），若为 0，则只进行一次初始发现 */
    private final long partitionDiscoveryIntervalMs;

    /** Flink 运行时提供的 SplitEnumeratorContext，用于管理 Reader 分配 */
    private final SplitEnumeratorContext<KafkaPartitionSplit> context;

    /** 表示数据流是有界（Batch）还是无界（Streaming） */
    private final Boundedness boundedness;

    /** 记录已经分配给 SourceReader 的 Kafka 分区 */
    private final Set<TopicPartition> assignedPartitions;

    /**
     * 存储初始化时发现但尚未分配给 Reader 的 Kafka 分区。
     * <p>当 SourceReader 启动后，会从该集合中领取任务。</p>
     */
    private final Set<TopicPartition> unassignedInitialPartitions;

    /**
     * 存储已发现并初始化的 Kafka 分区（KafkaPartitionSplit），
     * 但尚未找到合适的 Reader 进行分配的任务。
     */
    private final Map<Integer, Set<KafkaPartitionSplit>> pendingPartitionSplitAssignment;

    /** 当前 KafkaSource 绑定的 Consumer Group ID */
    private final String consumerGroupId;

    /** Kafka 管理客户端（AdminClient），用于查询 Kafka 主题分区信息 */
    private AdminClient adminClient;

    /**
     * 是否已经完成了所有 Kafka 分区的发现，
     * 当 `partitionDiscoveryIntervalMs == 0` 时，初始化完成后会标记为 true。
     */
    private boolean noMoreNewPartitionSplits = false;

    /** 是否完成了初始 Kafka 分区的发现 */
    private boolean initialDiscoveryFinished;

    /**
     * KafkaSourceEnumerator 构造方法（无状态初始化）。
     *
     * @param subscriber 订阅 Kafka 主题的策略（如指定主题、正则匹配等）。
     * @param startingOffsetInitializer 指定起始 Offset 的策略（如 earliest, latest）。
     * @param stoppingOffsetInitializer 指定终止 Offset 的策略（用于有界流）。
     * @param properties Kafka Consumer 相关的配置参数。
     * @param context Flink 提供的 SplitEnumeratorContext（用于任务管理）。
     * @param boundedness 指定数据流是有界（Batch）还是无界（Streaming）。
     */
    public KafkaSourceEnumerator(
            KafkaSubscriber subscriber,
            OffsetsInitializer startingOffsetInitializer,
            OffsetsInitializer stoppingOffsetInitializer,
            Properties properties,
            SplitEnumeratorContext<KafkaPartitionSplit> context,
            Boundedness boundedness) {
        this(
                subscriber,
                startingOffsetInitializer,
                stoppingOffsetInitializer,
                properties,
                context,
                boundedness,
                new KafkaSourceEnumState(Collections.emptySet(), false));
    }

    /**
     * KafkaSourceEnumerator 构造方法（带初始状态）。
     *
     * @param subscriber 订阅 Kafka 主题的策略（如指定主题、正则匹配等）。
     * @param startingOffsetInitializer 指定起始 Offset 的策略（如 earliest, latest）。
     * @param stoppingOffsetInitializer 指定终止 Offset 的策略（用于有界流）。
     * @param properties Kafka Consumer 相关的配置参数。
     * @param context Flink 提供的 SplitEnumeratorContext（用于任务管理）。
     * @param boundedness 指定数据流是有界（Batch）还是无界（Streaming）。
     * @param kafkaSourceEnumState 传入的 KafkaSource 枚举器状态（恢复时使用）。
     */
    public KafkaSourceEnumerator(
            KafkaSubscriber subscriber,
            OffsetsInitializer startingOffsetInitializer,
            OffsetsInitializer stoppingOffsetInitializer,
            Properties properties,
            SplitEnumeratorContext<KafkaPartitionSplit> context,
            Boundedness boundedness,
            KafkaSourceEnumState kafkaSourceEnumState) {
        this.subscriber = subscriber;
        this.startingOffsetInitializer = startingOffsetInitializer;
        this.stoppingOffsetInitializer = stoppingOffsetInitializer;
        this.newDiscoveryOffsetsInitializer = OffsetsInitializer.earliest();
        this.properties = properties;
        this.context = context;
        this.boundedness = boundedness;

        // 从恢复状态中初始化已分配的 Kafka 分区
        this.assignedPartitions = new HashSet<>(kafkaSourceEnumState.assignedPartitions());

        // 初始化等待分配的分区映射
        this.pendingPartitionSplitAssignment = new HashMap<>();

        // 读取 Kafka 配置中的 `partition.discovery.interval.ms` 参数
        this.partitionDiscoveryIntervalMs =
                KafkaSourceOptions.getOption(
                        properties,
                        KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS,
                        Long::parseLong);

        // 读取 Kafka Consumer Group ID
        this.consumerGroupId = properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);

        // 从恢复状态中初始化未分配的 Kafka 分区
        this.unassignedInitialPartitions =
                new HashSet<>(kafkaSourceEnumState.unassignedInitialPartitions());

        // 记录是否已完成 Kafka 主题的初始分区发现
        this.initialDiscoveryFinished = kafkaSourceEnumState.initialDiscoveryFinished();
    }

    /**
     * 启动 KafkaSourceEnumerator，负责发现 Kafka 主题的分区并分配给 Flink 任务。
     *
     * <p>根据 {@link #partitionDiscoveryIntervalMs} 参数：
     * 1. 如果大于 0，则会定期进行 Kafka 分区发现（周期性调用）。
     * 2. 如果等于 0，则只执行一次 Kafka 主题的分区发现，不进行动态更新。
     *
     * <p>分区发现的调用链：
     * 1. 在 worker 线程调用 {@link #getSubscribedTopicPartitions} 获取 Kafka 主题的分区信息。
     * 2. 在 coordinator 线程调用 {@link #checkPartitionChanges} 检查分区变更情况。
     * 3. 在 worker 线程调用 {@link #initializePartitionSplits} 为新发现的分区初始化 Offset。
     * 4. 在 coordinator 线程调用 {@link #handlePartitionSplitChanges} 分配新的分区给 Flink 任务。
     */
    @Override
    public void start() {
        // 创建 Kafka AdminClient，用于查询 Kafka 主题的分区信息
        adminClient = getKafkaAdminClient();

        if (partitionDiscoveryIntervalMs > 0) {
            // 启动周期性 Kafka 主题发现
            LOG.info(
                    "启动 KafkaSourceEnumerator，消费组：{}，分区发现间隔：{} ms。",
                    consumerGroupId,
                    partitionDiscoveryIntervalMs);
            context.callAsync(
                    this::getSubscribedTopicPartitions,
                    this::checkPartitionChanges,
                    0,
                    partitionDiscoveryIntervalMs);
        } else {
            // 仅执行一次 Kafka 主题发现
            LOG.info("启动 KafkaSourceEnumerator，消费组：{}，无周期性分区发现。", consumerGroupId);
            context.callAsync(this::getSubscribedTopicPartitions, this::checkPartitionChanges);
        }
    }

    /**
     * 处理 Flink 任务请求新的 Kafka 分区（通常不会被调用）。
     *
     * <p>Kafka Source 采用“主动推送”策略，而不是等任务请求，因此此方法通常不会执行任何操作。
     */
    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // Kafka Source 采用主动推送的方式分配分片，而不是等待任务请求
    }

    /**
     * 重新分配失败任务的 Kafka 分区。
     *
     * <p>当某个 Flink Task 失败重启时，它之前分配的 Kafka 分区会重新加入待分配列表。
     * 该方法会：
     * 1. 将失败任务的 Kafka 分区加入 `pendingPartitionSplitAssignment`。
     * 2. 如果任务已经恢复，则立即重新分配 Kafka 分区给该任务。
     */
    @Override
    public void addSplitsBack(List<KafkaPartitionSplit> splits, int subtaskId) {
        addPartitionSplitChangeToPendingAssignments(splits);

        // 检查任务是否已经重新启动，如果是，则立即重新分配 Kafka 分区
        if (context.registeredReaders().containsKey(subtaskId)) {
            assignPendingPartitionSplits(Collections.singleton(subtaskId));
        }
    }

    /**
     * 当新的 Flink 任务（Reader）启动时，尝试为其分配 Kafka 分区。
     *
     * <p>该方法会检查 `pendingPartitionSplitAssignment` 是否有等待分配的 Kafka 分区，
     * 如果有，则立即为该任务分配 Kafka 分区进行消费。
     */
    @Override
    public void addReader(int subtaskId) {
        LOG.debug(
                "为 KafkaSourceEnumerator 添加新的 Reader（任务编号：{}，消费组：{}）。",
                subtaskId,
                consumerGroupId);
        assignPendingPartitionSplits(Collections.singleton(subtaskId));
    }

    /**
     * 生成 KafkaSourceEnumerator 的快照状态（Checkpoint）。
     *
     * <p>Flink 在执行 Checkpoint 时会调用该方法，记录：
     * - 已分配的 Kafka 分区（assignedPartitions）
     * - 未分配的 Kafka 分区（unassignedInitialPartitions）
     * - 是否已完成初始 Kafka 分区发现
     *
     * <p>这些信息用于 Flink 恢复任务时，确保 Kafka Source 可以继续消费 Kafka 数据，而不会重复读取或丢失数据。
     */
    @Override
    public KafkaSourceEnumState snapshotState(long checkpointId) throws Exception {
        return new KafkaSourceEnumState(
                assignedPartitions, unassignedInitialPartitions, initialDiscoveryFinished);
    }

    /**
     * 关闭 KafkaSourceEnumerator，释放 Kafka AdminClient 资源。
     */
    @Override
    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }

// ----------------- 私有方法 -------------------

    /**
     * 获取 Kafka 主题的分区信息（需要访问 Kafka）。
     *
     * <p>该方法运行在 worker 线程中，因为它涉及与 Kafka Broker 进行网络通信。
     *
     * @return Kafka 主题的分区集合 {@link TopicPartition}
     */
    private Set<TopicPartition> getSubscribedTopicPartitions() {
        return subscriber.getSubscribedTopicPartitions(adminClient);
    }

    /**
     * 检查 Kafka 主题的分区是否发生变化（新增或删除）。
     *
     * <p>该方法运行在 Flink 的协调线程（Coordinator Thread）。
     * 如果发现 Kafka 主题的分区发生变化（新增分区），则触发后续的分区初始化流程。
     *
     * @param fetchedPartitions 从 Kafka 获取的最新分区信息
     * @param t 如果 Kafka 查询失败，异常信息将存储在此变量中
     */
    private void checkPartitionChanges(Set<TopicPartition> fetchedPartitions, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("获取 Kafka 主题分区信息失败：", t);
        }

        if (!initialDiscoveryFinished) {
            // 记录初次发现但未分配的 Kafka 分区
            unassignedInitialPartitions.addAll(fetchedPartitions);
            initialDiscoveryFinished = true;
        }

        final PartitionChange partitionChange = getPartitionChange(fetchedPartitions);
        if (partitionChange.isEmpty()) {
            return;
        }
        context.callAsync(
                () -> initializePartitionSplits(partitionChange),
                this::handlePartitionSplitChanges);
    }

    /**
     * 为新发现的 Kafka 分区初始化 Offset 信息。
     *
     * <p>该方法运行在 worker 线程中，可能涉及 Kafka Broker 的网络 I/O 操作。
     * Flink 可能需要根据用户指定的 Offset 初始化策略（earliest / latest / timestamp）
     * 来获取 Kafka 主题的起始 Offset。
     *
     * @param partitionChange 包含新发现和被移除的 Kafka 分区信息
     * @return 已初始化的 Kafka 分区（KafkaPartitionSplit）
     */
    private PartitionSplitChange initializePartitionSplits(PartitionChange partitionChange) {
        Set<TopicPartition> newPartitions =
                Collections.unmodifiableSet(partitionChange.getNewPartitions());

        OffsetsInitializer.PartitionOffsetsRetriever offsetsRetriever = getOffsetsRetriever();

        // 为新发现的分区初始化 Offset（默认 earliest）
        Map<TopicPartition, Long> startingOffsets = new HashMap<>();
        startingOffsets.putAll(
                newDiscoveryOffsetsInitializer.getPartitionOffsets(newPartitions, offsetsRetriever));
        startingOffsets.putAll(
                startingOffsetInitializer.getPartitionOffsets(unassignedInitialPartitions, offsetsRetriever));

        // 获取终止 Offset（如果用户定义了 stoppingOffsetInitializer）
        Map<TopicPartition, Long> stoppingOffsets =
                stoppingOffsetInitializer.getPartitionOffsets(newPartitions, offsetsRetriever);

        Set<KafkaPartitionSplit> partitionSplits = new HashSet<>(newPartitions.size());
        for (TopicPartition tp : newPartitions) {
            Long startingOffset = startingOffsets.get(tp);
            long stoppingOffset =
                    stoppingOffsets.getOrDefault(tp, KafkaPartitionSplit.NO_STOPPING_OFFSET);
            partitionSplits.add(new KafkaPartitionSplit(tp, startingOffset, stoppingOffset));
        }
        return new PartitionSplitChange(partitionSplits, partitionChange.getRemovedPartitions());
    }


    /**
     * 处理 Kafka 主题分区的变化，将新发现的分区加入待分配队列，并尝试将其分配给已注册的 Flink 任务。
     *
     * <p>此方法只能在 **协调线程（Coordinator Thread）** 中调用，因为它会修改全局分区状态。</p>
     *
     * @param partitionSplitChange 包含新发现的 Kafka 分区信息
     * @param t 如果 worker 线程在发现分区时发生异常，该异常会被传递到此方法
     */
    private void handlePartitionSplitChanges(
            PartitionSplitChange partitionSplitChange, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("初始化 Kafka 分区失败：", t);
        }

        if (partitionDiscoveryIntervalMs <= 0) {
            LOG.debug("Kafka 分区发现已禁用。");
            noMoreNewPartitionSplits = true;
        }

        // 将新发现的 Kafka 分区添加到待分配任务队列
        addPartitionSplitChangeToPendingAssignments(partitionSplitChange.newPartitionSplits);

        // 尝试为所有已注册的 Flink 任务分配 Kafka 分区
        assignPendingPartitionSplits(context.registeredReaders().keySet());
    }

    /**
     * 将新发现的 Kafka 分区添加到待分配任务队列。
     *
     * <p>此方法只能在 **协调线程（Coordinator Thread）** 中调用。</p>
     *
     * @param newPartitionSplits 需要分配的 Kafka 分区
     */
    private void addPartitionSplitChangeToPendingAssignments(
            Collection<KafkaPartitionSplit> newPartitionSplits) {
        int numReaders = context.currentParallelism();

        for (KafkaPartitionSplit split : newPartitionSplits) {
            // 计算 Kafka 分区应该被分配到哪个 Flink 并行任务
            int ownerReader = getSplitOwner(split.getTopicPartition(), numReaders);

            // 将 Kafka 分区加入该任务的待分配队列
            pendingPartitionSplitAssignment
                    .computeIfAbsent(ownerReader, r -> new HashSet<>())
                    .add(split);
        }

        LOG.debug(
                "已分配 {} 个分区给 {} 个 Reader，消费组：{}。",
                newPartitionSplits,
                numReaders,
                consumerGroupId);
    }

    /**
     * 将待分配的 Kafka 分区分配给指定的 Flink 任务（Reader）。
     *
     * <p>此方法只能在 **协调线程（Coordinator Thread）** 中调用。</p>
     *
     * @param pendingReaders 需要分配 Kafka 分区的任务 ID 集合
     */
    private void assignPendingPartitionSplits(Set<Integer> pendingReaders) {
        Map<Integer, List<KafkaPartitionSplit>> incrementalAssignment = new HashMap<>();

        // 遍历所有待分配的 Reader，检查是否有待分配的 Kafka 分区
        for (int pendingReader : pendingReaders) {
            checkReaderRegistered(pendingReader);

            // 从待分配任务列表中移除该 Reader 的任务
            final Set<KafkaPartitionSplit> pendingAssignmentForReader =
                    pendingPartitionSplitAssignment.remove(pendingReader);

            if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
                // 将 Kafka 分区分配给该任务
                incrementalAssignment
                        .computeIfAbsent(pendingReader, (ignored) -> new ArrayList<>())
                        .addAll(pendingAssignmentForReader);

                // 将分配出去的 Kafka 分区标记为已分配
                pendingAssignmentForReader.forEach(
                        split -> {
                            assignedPartitions.add(split.getTopicPartition());
                            unassignedInitialPartitions.remove(split.getTopicPartition());
                        });
            }
        }

        // 进行实际的 Kafka 分区分配
        if (!incrementalAssignment.isEmpty()) {
            LOG.info("分配 Kafka 分区给 Reader {}", incrementalAssignment);
            context.assignSplits(new SplitsAssignment<>(incrementalAssignment));
        }

        // 如果 Kafka 分区发现已禁用，并且初始发现已经完成，则通知 Flink 任务不再有新分区
        if (noMoreNewPartitionSplits && boundedness == Boundedness.BOUNDED) {
            LOG.debug(
                    "所有 Kafka 分区已分配，发送 NoMoreSplitsEvent 事件给 Reader {}，消费组：{}。",
                    pendingReaders,
                    consumerGroupId);
            pendingReaders.forEach(context::signalNoMoreSplits);
        }
    }

    /**
     * 检查给定的 Reader 是否已经注册到 Flink 任务调度器。
     *
     * @param readerId Flink 任务的 ID
     */
    private void checkReaderRegistered(int readerId) {
        if (!context.registeredReaders().containsKey(readerId)) {
            throw new IllegalStateException(
                    String.format("Reader %d 尚未注册到 SourceCoordinator", readerId));
        }
    }

    /**
     * 计算 Kafka 主题的新增/移除分区情况。
     *
     * <p>遍历当前已知的 Kafka 分区，并与 `fetchedPartitions` 进行对比，找出新增加的分区
     * 以及已被删除的分区。</p>
     *
     * @param fetchedPartitions 从 Kafka 查询到的最新分区集合
     * @return 包含新增和移除分区的对象 {@link PartitionChange}
     */
    @VisibleForTesting
    PartitionChange getPartitionChange(Set<TopicPartition> fetchedPartitions) {
        final Set<TopicPartition> removedPartitions = new HashSet<>();

        // 检查已分配的分区是否仍然存在
        Consumer<TopicPartition> dedupOrMarkAsRemoved =
                (tp) -> {
                    if (!fetchedPartitions.remove(tp)) {
                        removedPartitions.add(tp);
                    }
                };

        assignedPartitions.forEach(dedupOrMarkAsRemoved);
        pendingPartitionSplitAssignment.forEach(
                (reader, splits) -> splits.forEach(split -> dedupOrMarkAsRemoved.accept(split.getTopicPartition())));

        if (!fetchedPartitions.isEmpty()) {
            LOG.info("发现新的 Kafka 分区：{}", fetchedPartitions);
        }
        if (!removedPartitions.isEmpty()) {
            LOG.info("检测到已移除的 Kafka 分区：{}", removedPartitions);
        }

        return new PartitionChange(fetchedPartitions, removedPartitions);
    }

    /**
     * 获取 Kafka AdminClient，用于查询 Kafka 主题的分区信息。
     *
     * @return Kafka AdminClient 实例
     */
    private AdminClient getKafkaAdminClient() {
        Properties adminClientProps = new Properties();
        deepCopyProperties(properties, adminClientProps);

        // 设置 Client ID 前缀
        String clientIdPrefix =
                adminClientProps.getProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key());
        adminClientProps.setProperty(
                ConsumerConfig.CLIENT_ID_CONFIG, clientIdPrefix + "-enumerator-admin-client");

        return AdminClient.create(adminClientProps);
    }

    /**
     * 获取 Kafka 偏移量初始化器，用于查询 Kafka 分区的起始偏移量。
     *
     * @return Kafka 偏移量初始化器实例
     */
    private OffsetsInitializer.PartitionOffsetsRetriever getOffsetsRetriever() {
        String groupId = properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        return new PartitionOffsetsRetrieverImpl(adminClient, groupId);
    }


    /**
     * 确定指定的 Kafka 分区应分配给哪个 Flink 子任务（subtask）。
     *
     * <p>分区分配规则：
     * <ul>
     *     <li>1. 主题的所有分区应 **均匀** 分布到不同的 subtasks 上。</li>
     *     <li>2. 分区按照 **顺时针 Round-Robin** 方式分配，以 topic 的 hash 值作为起始索引，
     *         然后根据分区 ID 进行偏移分配。</li>
     * </ul>
     *
     * <p>具体步骤：
     * <ol>
     *     <li>计算 topic 名称的哈希值，并对 reader 数量取模，得到起始索引。</li>
     *     <li>使用 Kafka 分区 ID 作为偏移量，从起始索引开始顺时针分配分区。</li>
     * </ol>
     *
     * @param tp Kafka 分区信息（包含 topic 名称和分区号）。
     * @param numReaders Flink 任务的并行度，即 reader 的总数。
     * @return 该 Kafka 分区应该分配到的 Flink 任务 ID。
     */
    @VisibleForTesting
    static int getSplitOwner(TopicPartition tp, int numReaders) {
        // 计算 topic 名称的哈希值，并确保为正数，然后对 reader 总数取模，确定分配起始索引
        int startIndex = ((tp.topic().hashCode() * 31) & 0x7FFFFFFF) % numReaders;

        // 由于 Kafka 分区 ID 是从 0 开始的连续递增整数，可以直接作为偏移量，从 startIndex 进行 Round-Robin 分配
        return (startIndex + tp.partition()) % numReaders;
    }

    /**
     * 深拷贝 `Properties` 对象，将 `from` 的所有键值复制到 `to`。
     *
     * @param from 原始 `Properties` 对象。
     * @param to 目标 `Properties` 对象。
     */
    @VisibleForTesting
    static void deepCopyProperties(Properties from, Properties to) {
        for (String key : from.stringPropertyNames()) {
            to.setProperty(key, from.getProperty(key));
        }
    }

// --------------- 内部类定义 ---------------

    /**
     * `PartitionChange` 类用于存储 Kafka 主题的分区变化情况（新增或删除的分区）。
     */
    @VisibleForTesting
    static class PartitionChange {
        private final Set<TopicPartition> newPartitions;   // 新增的 Kafka 分区
        private final Set<TopicPartition> removedPartitions; // 已删除的 Kafka 分区

        /**
         * 构造方法，存储新增和移除的 Kafka 分区信息。
         *
         * @param newPartitions 新发现的 Kafka 分区。
         * @param removedPartitions 已被移除的 Kafka 分区。
         */
        PartitionChange(Set<TopicPartition> newPartitions, Set<TopicPartition> removedPartitions) {
            this.newPartitions = newPartitions;
            this.removedPartitions = removedPartitions;
        }

        /** @return 新发现的 Kafka 分区集合。 */
        public Set<TopicPartition> getNewPartitions() {
            return newPartitions;
        }

        /** @return 已移除的 Kafka 分区集合。 */
        public Set<TopicPartition> getRemovedPartitions() {
            return removedPartitions;
        }

        /** @return 如果没有新发现或移除的分区，则返回 `true`，否则返回 `false`。 */
        public boolean isEmpty() {
            return newPartitions.isEmpty() && removedPartitions.isEmpty();
        }
    }

    /**
     * `PartitionSplitChange` 记录了 Kafka 主题分区的变更信息，
     * 其中包含新增的 `KafkaPartitionSplit` 和已移除的 `TopicPartition`。
     */
    private static class PartitionSplitChange {
        private final Set<KafkaPartitionSplit> newPartitionSplits; // 新增的 Kafka 分区
        private final Set<TopicPartition> removedPartitions; // 已移除的 Kafka 分区

        /**
         * 构造方法，存储分区变化情况。
         *
         * @param newPartitionSplits 新增的 Kafka 分区分片（Splits）。
         * @param removedPartitions 已移除的 Kafka 主题分区。
         */
        private PartitionSplitChange(
                Set<KafkaPartitionSplit> newPartitionSplits,
                Set<TopicPartition> removedPartitions) {
            this.newPartitionSplits = Collections.unmodifiableSet(newPartitionSplits);
            this.removedPartitions = Collections.unmodifiableSet(removedPartitions);
        }
    }

    /**
     * `PartitionOffsetsRetrieverImpl` 负责使用 Kafka AdminClient 获取 Kafka 主题的偏移量信息。
     */
    @VisibleForTesting
    public static class PartitionOffsetsRetrieverImpl
            implements OffsetsInitializer.PartitionOffsetsRetriever, AutoCloseable {
        private final AdminClient adminClient; // Kafka AdminClient，用于查询 Kafka 主题信息
        private final String groupId; // Kafka 消费者组 ID

        /**
         * 构造方法，初始化 Kafka AdminClient 和消费组 ID。
         *
         * @param adminClient Kafka AdminClient 实例。
         * @param groupId Kafka 消费组 ID。
         */
        public PartitionOffsetsRetrieverImpl(AdminClient adminClient, String groupId) {
            this.adminClient = adminClient;
            this.groupId = groupId;
        }

        /**
         * 获取 Kafka 消费组提交的偏移量。
         *
         * @param partitions 需要查询偏移量的 Kafka 主题分区。
         * @return 返回 Map，键为 `TopicPartition`，值为该分区的提交偏移量。
         */
        @Override
        public Map<TopicPartition, Long> committedOffsets(Collection<TopicPartition> partitions) {
            // 创建 Kafka AdminClient 查询选项，限定查询的 Kafka 分区
            ListConsumerGroupOffsetsOptions options =
                    new ListConsumerGroupOffsetsOptions()
                            .topicPartitions(new ArrayList<>(partitions));
            try {
                return adminClient
                        .listConsumerGroupOffsets(groupId, options)
                        .partitionsToOffsetAndMetadata()
                        .thenApply(
                                result -> {
                                    Map<TopicPartition, Long> offsets = new HashMap<>();
                                    result.forEach(
                                            (tp, oam) -> {
                                                if (oam != null) {
                                                    offsets.put(tp, oam.offset());
                                                }
                                            });
                                    return offsets;
                                })
                        .get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlinkRuntimeException(
                        "查询 Kafka 消费组 " + groupId + " 的偏移量时被中断", e);
            } catch (ExecutionException e) {
                throw new FlinkRuntimeException(
                        "获取 Kafka 消费组 " + groupId + " 提交的偏移量失败：", e);
            }
        }


        /**
         * 查询 Kafka 主题分区的偏移量信息。
         *
         * <p>该方法允许查找：
         * <ul>
         *     <li>分区的 **起始（earliest）偏移量**</li>
         *     <li>分区的 **最新（latest）偏移量**</li>
         *     <li>分区中 **匹配指定时间戳的偏移量**</li>
         * </ul>
         *
         * @param topicPartitionOffsets 需要查询偏移量的 Kafka 主题分区映射，每个分区对应一个 {@link OffsetSpec} 查询条件。
         * @return 包含查询结果的 Map，键为 {@link TopicPartition}，值为 {@link ListOffsetsResult.ListOffsetsResultInfo}（包含偏移量、时间戳等信息）。
         * @see KafkaAdminClient#listOffsets(Map)
         */
        private Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> listOffsets(
                Map<TopicPartition, OffsetSpec> topicPartitionOffsets) {
            try {
                return adminClient
                        .listOffsets(topicPartitionOffsets) // 通过 Kafka AdminClient 查询偏移量
                        .all()
                        .thenApply(
                                result -> {
                                    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets = new HashMap<>();
                                    result.forEach(
                                            (tp, listOffsetsResultInfo) -> {
                                                if (listOffsetsResultInfo != null) {
                                                    offsets.put(tp, listOffsetsResultInfo);
                                                }
                                            });
                                    return offsets;
                                })
                        .get(); // 阻塞获取查询结果
            } catch (InterruptedException e) {
                // 线程中断异常，恢复中断标志
                Thread.currentThread().interrupt();
                throw new FlinkRuntimeException(
                        "在获取 Kafka 主题分区偏移量时被中断：" + topicPartitionOffsets, e);
            } catch (ExecutionException e) {
                throw new FlinkRuntimeException(
                        "获取 Kafka 主题分区偏移量失败：" + topicPartitionOffsets, e);
            }
        }

        /**
         * 查询 Kafka 主题分区的特定类型偏移量，如起始偏移量（earliest）、最新偏移量（latest）。
         *
         * @param partitions 需要查询的 Kafka 主题分区。
         * @param offsetSpec 偏移量类型（如 `OffsetSpec.earliest()` 或 `OffsetSpec.latest()`）。
         * @return 返回 Map，键为 {@link TopicPartition}，值为偏移量（Long）。
         */
        private Map<TopicPartition, Long> listOffsets(
                Collection<TopicPartition> partitions, OffsetSpec offsetSpec) {
            return listOffsets(
                    partitions.stream()
                            .collect(
                                    Collectors.toMap(
                                            partition -> partition, __ -> offsetSpec)))
                    .entrySet().stream()
                    .collect(
                            Collectors.toMap(
                                    Map.Entry::getKey, entry -> entry.getValue().offset()));
        }

        /**
         * 获取 Kafka 分区的最新偏移量（latest offset）。
         *
         * <p>通常用于流式处理场景，表示从 Kafka 主题的最新位置开始消费数据。</p>
         *
         * @param partitions 需要查询的 Kafka 主题分区集合。
         * @return 返回 Map，键为 {@link TopicPartition}，值为最新偏移量（long）。
         */
        @Override
        public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
            return listOffsets(partitions, OffsetSpec.latest());
        }

        /**
         * 获取 Kafka 分区的起始偏移量（earliest offset）。
         *
         * <p>通常用于从 Kafka 主题的最早数据位置开始消费。</p>
         *
         * @param partitions 需要查询的 Kafka 主题分区集合。
         * @return 返回 Map，键为 {@link TopicPartition}，值为最早偏移量（long）。
         */
        @Override
        public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
            return listOffsets(partitions, OffsetSpec.earliest());
        }

        /**
         * 获取 Kafka 主题分区中最接近指定时间戳的偏移量。
         *
         * <p>该方法用于时间戳定位消费，例如，想要从某个时间点开始消费 Kafka 数据。</p>
         *
         * <p>**实现逻辑**：
         * <ul>
         *     <li>1. 构造 `OffsetSpec.forTimestamp(timestamp)` 查询指定时间戳的偏移量。</li>
         *     <li>2. 调用 `listOffsets()` 获取 Kafka 返回的偏移量信息。</li>
         *     <li>3. 过滤掉 Kafka 返回的 **无效偏移量（负值）**，避免错误数据。</li>
         *     <li>4. 返回 `OffsetAndTimestamp`，其中包含：偏移量、时间戳、Kafka Leader Epoch（用于分区领导者变更）。</li>
         * </ul>
         *
         * @param timestampsToSearch 需要查询的 Kafka 主题分区及对应的时间戳。
         * @return 返回 Map，键为 {@link TopicPartition}，值为 {@link OffsetAndTimestamp}（包含偏移量、时间戳、Leader Epoch）。
         */
        @Override
        public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
                Map<TopicPartition, Long> timestampsToSearch) {
            return listOffsets(
                    timestampsToSearch.entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry -> OffsetSpec.forTimestamp(entry.getValue()))))
                    .entrySet().stream()
                    // 过滤掉无效偏移量（小于 0），因为如果 Kafka 没有匹配的时间戳数据，可能返回负偏移量
                    .filter(entry -> entry.getValue().offset() >= 0)
                    .collect(
                            Collectors.toMap(
                                    Map.Entry::getKey,
                                    entry -> new OffsetAndTimestamp(
                                            entry.getValue().offset(),
                                            entry.getValue().timestamp(),
                                            entry.getValue().leaderEpoch())));
        }

        /**
         * 关闭 Kafka AdminClient 连接，释放资源。
         */
        @Override
        public void close() throws Exception {
            adminClient.close(Duration.ZERO);
        }

    }
}
