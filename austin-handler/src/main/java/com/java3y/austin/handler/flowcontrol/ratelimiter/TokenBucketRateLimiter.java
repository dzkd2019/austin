package com.java3y.austin.handler.flowcontrol.ratelimiter;

import com.java3y.austin.handler.flowcontrol.config.RateLimiterConfig;
import com.java3y.austin.handler.flowcontrol.config.TokenBucketRateLimiterConfig;

import java.util.concurrent.locks.ReentrantLock;

public class TokenBucketRateLimiter {
    // ================= 配置参数 =================
    private volatile double maxTokens;       // 桶的最大容量（决定突发流量的能力）
    private volatile double tokensPerMs;     // 每毫秒生成的令牌数（生成速率）

    // ================= 运行时状态 =================
    private double storedTokens;             // 当前桶内剩余的令牌数
    private long lastRefillTimeMs;           // 上次填充令牌的时间戳

    // 使用 ReentrantLock 替代 synchronized，绝对防止 Pinning
    private final ReentrantLock lock = new ReentrantLock();

    private static final double MAX_TOKENS = 1000;
    private static final double MAX_QPS = 100;

    public TokenBucketRateLimiter(RateLimiterConfig config) {
        this.lastRefillTimeMs = System.currentTimeMillis();
        this.storedTokens = MAX_TOKENS;

        if (config instanceof TokenBucketRateLimiterConfig tc) {
            setRate(tc);
        }
        setRate(new TokenBucketRateLimiterConfig(MAX_QPS, MAX_TOKENS));
    }

    /**
     * 动态更新限流参数（配置中心回调调用）
     */
    public void setRate(TokenBucketRateLimiterConfig config) {
        lock.lock();
        try {
            this.tokensPerMs = config.qps() / 1000.0;
            this.maxTokens = config.burst();
            // 如果新的容量变小，截断当前的多余令牌
            this.storedTokens = Math.min(this.storedTokens, this.maxTokens);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取令牌（阻塞等待直到可用）
     */
    public double acquire() throws InterruptedException {
        long sleepTimeMs = 0;

        // 【极速临界区】：这里面只有纯内存加减法，耗时在纳秒级别
        lock.lock();
        try {
            long now = System.currentTimeMillis();

            // 1. 懒加载计算：根据时间流逝填充令牌
            long elapsedTime = Math.max(0, now - lastRefillTimeMs);
            storedTokens = Math.min(maxTokens, storedTokens + elapsedTime * tokensPerMs);
            lastRefillTimeMs = now;

            // 2. 消费令牌
            if (storedTokens >= 1.0) {
                // 令牌充足，直接扣减，无需休眠
                storedTokens -= 1.0;
            } else {
                // 令牌不足，计算【赤字】并预支未来
                double deficit = 1.0 - storedTokens;
                sleepTimeMs = (long) (deficit / tokensPerMs);

                // 清空当前令牌，并将上次填充时间推迟到未来（也就是你需要偿还的时间）
                storedTokens = 0.0;
                lastRefillTimeMs = now + sleepTimeMs;
            }
        } finally {
            lock.unlock();
        }

        // 【虚拟线程魔法】：在锁的外部进行等待
        // 遇到 sleep，当前的虚拟线程瞬间挂起并让出底层 CPU，绝不会阻塞系统！
        if (sleepTimeMs > 0) {
            Thread.sleep(sleepTimeMs);
        }

        return storedTokens;
    }
}
