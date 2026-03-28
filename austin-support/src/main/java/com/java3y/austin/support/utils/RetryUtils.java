package com.java3y.austin.support.utils;

import com.java3y.austin.common.exception.CommonException;
import com.java3y.austin.common.exception.NetWorkTimeoutException;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class RetryUtils {

    private static final long MAX_BACKOFF_MS = 60_000;

    /**
     * 针对云服务调用的轻量级虚拟线程重试执行器
     *
     * @param maxRetries  最大重试次数
     * @param baseDelayMs 基础退避时间（毫秒）
     * @param action      真正要执行的云端调用逻辑
     */
    public static void executeWithRetry(int maxRetries, long baseDelayMs, Runnable action) {
        submitWithRetry(maxRetries, baseDelayMs, () -> {
            action.run();
            return null; // Callable 需要返回值，对于 Runnable 我们直接返回 null 即可
        });
    }

    public static <T> T submitWithRetry(int maxRetries, long baseDelayMs, Callable<T> action) {
        int attempt = 0;

        while (true) {
            attempt++;
            try {
                // 1. 执行真实的业务逻辑
                return action.call(); // 如果成功，直接结束并返回

            } catch (Exception e) {
                // 2. 异常拦截与判断
                if (!isRetriable(e)) {
                    log.error("遇到不可重试的致命异常，直接放弃发送！异常: {}", e.getMessage());
                    throw new RuntimeException("不可重试的异常", e); // 直接抛出，不再重试
                }

                if (attempt >= maxRetries) {
                    log.error("云服务调用失败，已达到最大重试次数 {}，放弃重试。最后一次异常: {}", maxRetries, e.getMessage());
                    throw new RuntimeException("达到最大重试次数，放弃重试", e);
                }

                // 3. 计算退避时间 (指数退避 + 随机抖动)
                // 公式: baseDelay * 2^(attempt-1) + random(0, 500)
                long exponentialDelay = baseDelayMs * (1L << (attempt - 1));
                long boundedDelay = Math.min(exponentialDelay, MAX_BACKOFF_MS);
                long jitter = ThreadLocalRandom.current().nextLong(0, 500);
                long sleepTime = boundedDelay + jitter;

                log.warn("云服务调用发生瞬时故障 (尝试 {}/{})，将在 {} 毫秒后重试。异常原因: {}",
                        attempt, maxRetries, sleepTime, e.getMessage());

                try {
                    // 4. 【虚拟线程的核心魔法】
                    // 这里的 sleep() 会让当前虚拟线程优雅地卸载并挂起，绝对不会阻塞操作系统的底层线程！
                    // 10 万个虚拟线程在这里 sleep()，也只是堆内存里的 10 万个对象而已。
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    log.info("重试等待期间收到中断信号，立刻终止重试。");
                    Thread.currentThread().interrupt(); // 恢复中断标志位
                    throw new RuntimeException("重试被中断", ie);
                }
            }
        }
    }


    /**
     * 判断异常是否属于“可重试”的瞬时网络故障
     */
    private static boolean isRetriable(Exception e) {
        // 精准匹配可重试的网络瞬时故障，避免将 FileNotFoundException 等永久性错误误判为可重试
        // ConnectException / SocketException 已覆盖连接拒绝、连接重置等场景；
        // SocketTimeoutException 覆盖读写超时；
        // NoRouteToHostException 是 SocketException 的子类，无需单独列出。
        if (e instanceof java.net.SocketTimeoutException
                || e instanceof java.net.SocketException
                || e instanceof NetWorkTimeoutException) {

            return true;
        }
        if (e instanceof CommonException ce) {
            String code = ce.getCode();
            return switch (code) {
                case "-1", "500" -> true;
                default -> false;
            };
        }
        return false;
    }
}
