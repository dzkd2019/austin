package com.java3y.austin.handler.monitoring;

import com.java3y.austin.handler.backpressure.VirtualThreadBackPressureManager;
import com.java3y.austin.handler.utils.GroupIdMappingUtils;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 背压管理器 Micrometer 指标绑定器
 *
 * <p>为每个 Kafka 消费者组注册以下 Gauge 指标：
 * <ul>
 *   <li>{@code austin.backpressure.in.flight}  – 当前飞行中（未完成处理）的任务数</li>
 *   <li>{@code austin.backpressure.high.water.mark} – 当前高水位线</li>
 *   <li>{@code austin.backpressure.low.water.mark}  – 当前低水位线</li>
 *   <li>{@code austin.backpressure.paused}           – 消费者是否处于暂停状态（1=暂停，0=运行）</li>
 * </ul>
 * 所有 Gauge 均通过回调函数实时读取，无需额外锁或缓存。
 *
 * @author 3y
 */
@Slf4j
@Component
public class BackPressureMetricsBinder implements MeterBinder {

    private static final String TAG_GROUP_ID = "group.id";

    private final VirtualThreadBackPressureManager backPressureManager;
    private final List<String> allGroupIds;

    public BackPressureMetricsBinder(VirtualThreadBackPressureManager backPressureManager) {
        this.backPressureManager = backPressureManager;
        this.allGroupIds = GroupIdMappingUtils.getAllGroupIds();
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        for (String groupId : allGroupIds) {
            registerGroupGauges(registry, groupId);
        }
        log.info("BackPressureMetricsBinder: 已为 {} 个消费者组注册背压指标", allGroupIds.size());
    }

    private void registerGroupGauges(MeterRegistry registry, String groupId) {
        // 当前飞行中任务数（实时水位）
        Gauge.builder("austin.backpressure.in.flight",
                        backPressureManager, mgr -> mgr.getInFlightCount(groupId))
                .description("当前飞行中（未完成处理）的任务数")
                .tag(TAG_GROUP_ID, groupId)
                .register(registry);

        // 高水位线
        Gauge.builder("austin.backpressure.high.water.mark",
                        backPressureManager, mgr -> mgr.getWaterMarkConfig(groupId).highWaterMark())
                .description("背压高水位线（达到后暂停 Kafka 消费）")
                .tag(TAG_GROUP_ID, groupId)
                .register(registry);

        // 低水位线
        Gauge.builder("austin.backpressure.low.water.mark",
                        backPressureManager, mgr -> mgr.getWaterMarkConfig(groupId).lowWaterMark())
                .description("背压低水位线（降至后恢复 Kafka 消费）")
                .tag(TAG_GROUP_ID, groupId)
                .register(registry);

        // 暂停状态（1=已暂停，0=正常消费）
        Gauge.builder("austin.backpressure.paused",
                        backPressureManager, mgr -> mgr.isPaused(groupId) ? 1.0 : 0.0)
                .description("Kafka 消费者是否处于背压暂停状态")
                .tag(TAG_GROUP_ID, groupId)
                .register(registry);
    }
}
