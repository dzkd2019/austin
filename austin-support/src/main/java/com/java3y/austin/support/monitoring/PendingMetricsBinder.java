package com.java3y.austin.support.monitoring;

import com.java3y.austin.support.mq.MqRateLimiter;
import com.java3y.austin.support.pending.AbstractLazyPending;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.BlockingQueue;

/**
 * Pending 队列与 MQ 限流 Micrometer 指标绑定器
 *
 * <p>从 {@link AbstractLazyPending#PENDING_REGISTRY} 中读取所有已注册的 Pending 实例，
 * 为每个实例注册以下 Gauge：
 * <ul>
 *   <li>{@code austin.pending.queue.size}                – 当前队列中待处理元素数量</li>
 *   <li>{@code austin.pending.queue.remaining.capacity}  – 队列剩余容量</li>
 * </ul>
 *
 * <p>MQ 全局限流信号量指标（原来绑定到 AbstractLazyPending.inFlightBatches，现已迁移至
 * {@link MqRateLimiter}）：
 * <ul>
 *   <li>{@code austin.mq.semaphore.permits} – 信号量剩余可用许可数（许可耗尽表示下游满载）</li>
 * </ul>
 *
 * <p>所有 Gauge 均通过回调函数实时读取，不持有额外强引用，不阻塞采集线程。
 *
 * @author 3y
 */
@Slf4j
@Component
public class PendingMetricsBinder implements MeterBinder {

    private static final String TAG_PENDING_CLASS = "pending.class";

    @Autowired
    private MqRateLimiter mqRateLimiter;

    @Override
    @SuppressWarnings("rawtypes")
    public void bindTo(MeterRegistry registry) {
        // 注意：此处遍历的是注册表的快照（binder 的 bindTo 在 actuator 初始化后调用）。
        // 因为所有 Pending bean 均在 Spring 上下文初始化完成前完成 @PostConstruct，
        // 所以此时注册表已包含全部 Pending 实例。
        for (AbstractLazyPending pending : AbstractLazyPending.PENDING_REGISTRY) {
            String className = pending.getClass().getSimpleName();
            registerPendingGauges(registry, pending, className);
        }

        // MQ 全局限流信号量指标（已从 AbstractLazyPending 迁移至 MqRateLimiter）
        Gauge.builder("austin.mq.semaphore.permits", mqRateLimiter,
                        r -> (double) r.getSemaphore().availablePermits())
                .description("MQ 全局限流信号量剩余可用许可数（许可耗尽表示 MQ 下游满载）")
                .register(registry);

        log.info("PendingMetricsBinder: 已为 {} 个 Pending 实例及 MQ 限流信号量注册指标",
                AbstractLazyPending.PENDING_REGISTRY.size());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void registerPendingGauges(MeterRegistry registry, AbstractLazyPending pending,
                                       String className) {
        // 队列当前元素数量
        Gauge.builder("austin.pending.queue.size", pending,
                        p -> (double) safeGetQueueSize(p))
                .description("Pending 阻塞队列当前元素数量")
                .tag(TAG_PENDING_CLASS, className)
                .register(registry);

        // 队列剩余容量（capacity - size）
        Gauge.builder("austin.pending.queue.remaining.capacity", pending,
                        p -> (double) safeGetRemainingCapacity(p))
                .description("Pending 阻塞队列剩余可用容量")
                .tag(TAG_PENDING_CLASS, className)
                .register(registry);
    }

    @SuppressWarnings("rawtypes")
    private int safeGetQueueSize(AbstractLazyPending pending) {
        BlockingQueue<?> queue = safeGetQueue(pending);
        return queue != null ? queue.size() : 0;
    }

    @SuppressWarnings("rawtypes")
    private int safeGetRemainingCapacity(AbstractLazyPending pending) {
        BlockingQueue<?> queue = safeGetQueue(pending);
        return queue != null ? queue.remainingCapacity() : 0;
    }

    @SuppressWarnings("rawtypes")
    private BlockingQueue<?> safeGetQueue(AbstractLazyPending pending) {
        if (pending.getPendingParam() == null) {
            return null;
        }
        return pending.getPendingParam().getQueue();
    }
}

