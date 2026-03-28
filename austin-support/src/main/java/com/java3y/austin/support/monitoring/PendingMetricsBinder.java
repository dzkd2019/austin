package com.java3y.austin.support.monitoring;

import com.java3y.austin.support.mq.MqRateLimiter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * MQ 限流 Micrometer 指标绑定器
 *
 * <p>当前仅暴露全局 MQ 限流信号量指标（已从 AbstractLazyPending 迁移至
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

    @Autowired
    private MqRateLimiter mqRateLimiter;

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("austin.mq.semaphore.permits", mqRateLimiter,
                        r -> (double) r.getSemaphore().availablePermits())
                .description("MQ 全局限流信号量剩余可用许可数（许可耗尽表示 MQ 下游满载）")
                .register(registry);

        log.info("PendingMetricsBinder: 已注册 MQ 限流信号量指标");
    }
}
