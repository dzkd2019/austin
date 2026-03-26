package com.java3y.austin.handler.monitoring;

import com.java3y.austin.handler.utils.GroupIdMappingUtils;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Kafka 消费者组 Lag Micrometer 指标绑定器
 *
 * <p>设计要点（高并发安全）：
 * <ul>
 *   <li>Gauge 回调仅读取 {@code ConcurrentHashMap} 中的缓存值，
 *       绝不发起网络 I/O，因此 Prometheus 抓取线程不会被阻塞。</li>
 *   <li>单独的低优先级后台线程每隔 {@code lag-refresh-interval-seconds}（默认30s）
 *       通过复用的 Kafka AdminClient 异步刷新 lag 值。</li>
 *   <li>AdminClient 创建一次，复用直到 bean 销毁，避免频繁创建/关闭的开销。</li>
 * </ul>
 *
 * @author 3y
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "austin.mq.pipeline", havingValue = "kafka")
public class KafkaMetricsBinder implements MeterBinder {

    private static final String TAG_GROUP_ID = "group.id";
    private static final String TAG_TOPIC = "topic";

    /** groupId -> 缓存的 lag 值（非负；-1 表示上次采集失败） */
    private final Map<String, Double> lagCache = new ConcurrentHashMap<>();

    private final KafkaAdmin kafkaAdmin;
    private final List<String> allGroupIds;
    private final long lagRefreshIntervalSeconds;

    @Value("${austin.business.topic.name:austinBusiness}")
    private String businessTopic;

    /** 复用的 AdminClient（懒加载，首次刷新时初始化） */
    private volatile AdminClient adminClient;
    private final Object adminClientLock = new Object();

    private final ScheduledExecutorService lagRefreshScheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = Thread.ofPlatform().name("kafka-lag-refresher").build();
                t.setDaemon(true);
                t.setPriority(Thread.MIN_PRIORITY);
                return t;
            });

    public KafkaMetricsBinder(KafkaAdmin kafkaAdmin,
                              @Value("${austin.message-send.kafka-metrics.lag-refresh-interval-seconds:30}")
                              long lagRefreshIntervalSeconds) {
        this.kafkaAdmin = kafkaAdmin;
        this.allGroupIds = GroupIdMappingUtils.getAllGroupIds();
        this.lagRefreshIntervalSeconds = lagRefreshIntervalSeconds;
        // 初始化缓存
        allGroupIds.forEach(gid -> lagCache.put(gid, 0.0));
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        // 为每个消费者组注册 Gauge（从缓存读取，非阻塞）
        for (String groupId : allGroupIds) {
            Gauge.builder("austin.kafka.consumer.lag",
                            lagCache, cache -> cache.getOrDefault(groupId, 0.0))
                    .description("Kafka 消费者组 Lag（消息积压量）")
                    .tag(TAG_GROUP_ID, groupId)
                    .tag(TAG_TOPIC, businessTopic)
                    .register(registry);
        }

        // 启动后台刷新任务
        lagRefreshScheduler.scheduleWithFixedDelay(
                this::refreshLag,
                5,
                lagRefreshIntervalSeconds,
                TimeUnit.SECONDS);

        log.info("KafkaMetricsBinder: 已为 {} 个消费者组注册 Lag 指标，刷新间隔 {}s",
                allGroupIds.size(), lagRefreshIntervalSeconds);
    }

    /**
     * 后台异步刷新各消费者组 lag 值，结果写入 lagCache。
     * AdminClient 调用是阻塞 I/O，故隔离在专用后台线程，不影响 Prometheus 抓取。
     */
    private void refreshLag() {
        try {
            AdminClient client = getOrCreateAdminClient();
            for (String groupId : allGroupIds) {
                try {
                    ListConsumerGroupOffsetsResult offsetsResult =
                            client.listConsumerGroupOffsets(groupId);
                    Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                            offsetsResult.partitionsToOffsetAndMetadata()
                                    .get(10, TimeUnit.SECONDS);

                    if (committedOffsets == null || committedOffsets.isEmpty()) {
                        lagCache.put(groupId, 0.0);
                        continue;
                    }

                    // 获取 end offsets
                    Map<TopicPartition, Long> endOffsets = client
                            .listOffsets(buildLatestOffsetSpecs(committedOffsets))
                            .all()
                            .get(10, TimeUnit.SECONDS)
                            .entrySet().stream()
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    e -> e.getValue().offset()));

                    // 计算 lag = sum(endOffset - committedOffset) over all partitions
                    double totalLag = committedOffsets.entrySet().stream()
                            .mapToDouble(entry -> {
                                TopicPartition tp = entry.getKey();
                                long committed = entry.getValue().offset();
                                long end = endOffsets.getOrDefault(tp, committed);
                                return Math.max(0, end - committed);
                            })
                            .sum();

                    lagCache.put(groupId, totalLag);
                } catch (Exception e) {
                    log.warn("KafkaMetricsBinder: 刷新 groupId={} 的 lag 失败: {}", groupId, e.getMessage());
                    lagCache.put(groupId, -1.0);
                    // AdminClient 可能已损坏，重置以便下次重新创建
                    resetAdminClient();
                }
            }
        } catch (Exception e) {
            log.error("KafkaMetricsBinder: 获取 AdminClient 失败，本次 lag 刷新跳过", e);
        }
    }

    private AdminClient getOrCreateAdminClient() {
        if (adminClient == null) {
            synchronized (adminClientLock) {
                if (adminClient == null) {
                    adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
                    log.info("KafkaMetricsBinder: AdminClient 初始化完成");
                }
            }
        }
        return adminClient;
    }

    private void resetAdminClient() {
        synchronized (adminClientLock) {
            if (adminClient != null) {
                try {
                    adminClient.close(java.time.Duration.ofSeconds(10));
                } catch (Exception ignored) {
                    // 静默关闭
                }
                adminClient = null;
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        lagRefreshScheduler.shutdownNow();
        resetAdminClient();
    }

    private Map<TopicPartition, OffsetSpec> buildLatestOffsetSpecs(
            Map<TopicPartition, OffsetAndMetadata> committed) {
        Map<TopicPartition, OffsetSpec> specs = new HashMap<>();
        committed.keySet().forEach(tp -> specs.put(tp, OffsetSpec.latest()));
        return specs;
    }
}
