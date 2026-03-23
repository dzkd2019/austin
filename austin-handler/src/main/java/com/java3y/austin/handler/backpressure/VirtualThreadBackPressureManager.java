package com.java3y.austin.handler.backpressure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Component
public class VirtualThreadBackPressureManager {
    private final AtomicInteger inFlightCount = new AtomicInteger(0);

    @Getter
    private volatile int highWaterMark = 100000;

    @Getter
    private volatile int lowWaterMark = 50000;

    private final Map<String, AtomicInteger> pendingCount = new ConcurrentHashMap<>();
    private final Map<String, GroupContext> groupContexts = new ConcurrentHashMap<>();

    private final KafkaListenerEndpointRegistry registry;

    private final ReentrantLock lock = new ReentrantLock();

    public VirtualThreadBackPressureManager(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    public void incrementAndCheckPause(String groupId) {
        GroupContext context = getGroupContext(groupId);
        int current = context.inFlightCount.incrementAndGet();

        // 【极其优雅的 CAS 控制】：
        // 只有当 current >= high 且 isPaused 原本为 false 时，才允许执行 pause()
        // 这意味着哪怕超过了 150000，底层的 container.pause() 也绝对只会执行一次！
        if (current >= highWaterMark && context.isPaused.compareAndSet(false, true)) {
            if (context.container != null) {
                log.warn("🚨 Group [{}] 触及高水位 ({} / {})，正式下达暂停消费指令", groupId, current, highWaterMark);
                context.container.pause();
            }
        }

    }

    public void decrementAndCheckResume(String groupId) {
        GroupContext context = getGroupContext(groupId);
        int current = context.inFlightCount.decrementAndGet();

        // 【极速回落】：
        // 只有当 isPaused 为 true 且回落到低水位时，才允许执行 resume()
        if (current <= lowWaterMark && context.isPaused.compareAndSet(true, false)) {
            if (context.container != null) {
                log.info("✅ Group [{}] 水位回落至低水位 ({} / {}), 恢复拉取消费", groupId, current, lowWaterMark);
                context.container.resume();
            }
        }
    }

    private GroupContext getGroupContext(String groupId) {
        return groupContexts.computeIfAbsent(groupId, k -> {
            MessageListenerContainer container = registry.getListenerContainer(k);
            if (container == null) {
                // Spring Kafka 默认的容器 ID 即为 groupId
                // 如果找不到，需要检查 @KafkaListener(id = "...", groupId = "...") 的配置
                log.warn("无法在 Registry 中找到 groupId [{}] 对应的 ListenerContainer", k);
            }
            return new GroupContext(container);
        });
    }

    private MessageListenerContainer getContainerByGroupId(String groupId) {
        if (groupId == null) {
            return null;
        }

        return registry.getListenerContainers().stream()
                .filter(c -> groupId.equals(c.getGroupId()))
                .findFirst()
                .orElse(null);
    }

    public void updateWaterMarks(int newLow, int newHigh) {
        if (newLow <= 0 || newHigh <= newLow) {
            throw new IllegalArgumentException("非法的动态水位参数: 低水位必须严格小于高水位");
        }

        lock.lock();
        try {
            log.info("🌊 动态调整水位: 高水位 {} -> {}, 低水位 {} -> {}",
                    this.highWaterMark, newHigh, this.lowWaterMark, newLow);

            this.highWaterMark = newHigh;
            this.lowWaterMark = newLow;

            // 遍历已缓存的上下文，触发可能的恢复动作
            groupContexts.forEach((groupId, context) -> {
                int current = context.inFlightCount.get();
                // 如果之前是暂停状态，且现在的水位已经满足新的低水位要求，立刻唤醒
                if (current <= this.lowWaterMark && context.isPaused.compareAndSet(true, false)) {
                    if (context.container != null) {
                        log.info("✅ 因水位阈值放宽，Group [{}] 提前恢复消费 ({} / {})", groupId, current, this.lowWaterMark);
                        context.container.resume();
                    }
                }
            });
        } finally {
            lock.unlock();
        }
    }

    private static class GroupContext {
        final AtomicInteger inFlightCount = new AtomicInteger(0);
        // 使用 CAS 原子布尔值，绝对防止重复 pause / resume
        final AtomicBoolean isPaused = new AtomicBoolean(false);
        // 缓存容器引用，避免每次 O(N) 遍历查找
        final MessageListenerContainer container;

        GroupContext(MessageListenerContainer container) {
            this.container = container;
        }
    }
}
