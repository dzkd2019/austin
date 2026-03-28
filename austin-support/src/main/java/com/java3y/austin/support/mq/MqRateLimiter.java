package com.java3y.austin.support.mq;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.Semaphore;

/**
 * MQ 全局限流令牌持有器
 *
 * <p>持有全局 {@link Semaphore}，由 {@code SendMqAction} 在向 MQ 发送前 acquire、发送后 release，
 * 确保无论 API 直接调用还是定时任务（cron）触发，所有发往 MQ 的操作均受统一信号量保护。
 * {@code PendingMetricsBinder} 通过此 Bean 将许可指标暴露给 Prometheus。
 *
 * @author 3y
 */
@Component
public class MqRateLimiter {

    /** 默认最大并发发送许可数 */
    private static final int DEFAULT_MQ_PERMITS = 1000;

    @Getter
    private final Semaphore semaphore;

    public MqRateLimiter(
            @Value("${austin.mq.rate-limiter.permits:" + DEFAULT_MQ_PERMITS + "}") int permits) {
        this.semaphore = new Semaphore(permits);
    }
}
