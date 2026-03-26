package com.java3y.austin.web.filter;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HTTP 接口层 Prometheus 指标采集过滤器
 *
 * <p>为每个 HTTP 请求采集以下指标：
 * <ul>
 *   <li>{@code austin.api.request.total}    – 请求总数（按 path、method、status 分类）</li>
 *   <li>{@code austin.api.request.error}    – 4xx/5xx 错误请求数</li>
 *   <li>{@code austin.api.request.duration} – 请求处理耗时（P50/P95/P99）</li>
 * </ul>
 *
 * <p>此 Filter 配合 {@link MdcEnrichFilter} 使用，Order 为
 * {@code HIGHEST_PRECEDENCE + 1}，保证 traceId 已写入 MDC。
 *
 * @author 3y
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
public class ApiMetricsFilter extends OncePerRequestFilter {

    private static final String TAG_PATH = "path";
    private static final String TAG_METHOD = "method";
    private static final String TAG_STATUS = "status";

    private final MeterRegistry meterRegistry;

    /** 缓存 Timer，避免每次请求创建 Timer 对象的开销 */
    private final Map<String, Timer> timerCache = new ConcurrentHashMap<>();
    /** 缓存总请求 Counter（按 path+method+status 维度） */
    private final Map<String, Counter> totalCounterCache = new ConcurrentHashMap<>();
    /** 缓存错误请求 Counter（按 path+method+status 维度） */
    private final Map<String, Counter> errorCounterCache = new ConcurrentHashMap<>();

    public ApiMetricsFilter(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        long startNano = System.nanoTime();
        try {
            filterChain.doFilter(request, response);
        } finally {
            long durationNano = System.nanoTime() - startNano;
            int status = response.getStatus();
            String path = normalizePath(request.getRequestURI());
            String method = request.getMethod();
            String statusStr = String.valueOf(status);

            // 请求耗时（按 path + method 聚合）
            String timerKey = method + ":" + path;
            timerCache.computeIfAbsent(timerKey, k ->
                    Timer.builder("austin.api.request.duration")
                            .description("Austin API 请求处理耗时")
                            .tag(TAG_PATH, path)
                            .tag(TAG_METHOD, method)
                            .publishPercentiles(0.5, 0.95, 0.99)
                            .publishPercentileHistogram()
                            .minimumExpectedValue(Duration.ofMillis(1))
                            .maximumExpectedValue(Duration.ofSeconds(60))
                            .register(meterRegistry)
            ).record(Duration.ofNanos(durationNano));

            // 请求计数（含状态码 tag，方便计算错误率）
            String totalCounterKey = method + ":" + path + ":" + statusStr;
            totalCounterCache.computeIfAbsent(totalCounterKey, k ->
                    Counter.builder("austin.api.request.total")
                            .description("Austin API 请求总数")
                            .tag(TAG_PATH, path)
                            .tag(TAG_METHOD, method)
                            .tag(TAG_STATUS, statusStr)
                            .register(meterRegistry)
            ).increment();

            // 额外的错误计数（4xx / 5xx）
            if (status >= 400) {
                errorCounterCache.computeIfAbsent(totalCounterKey, k ->
                        Counter.builder("austin.api.request.error")
                                .description("Austin API 4xx/5xx 错误请求数")
                                .tag(TAG_PATH, path)
                                .tag(TAG_METHOD, method)
                                .tag(TAG_STATUS, statusStr)
                                .register(meterRegistry)
                ).increment();
            }
        }
    }

    /**
     * 路径归一化：将动态路径片段替换为占位符，避免高基数 Tag 导致内存泄漏。
     * <ul>
     *   <li>/message/12345         -> /message/{id}</li>
     *   <li>/user/a1b2c3d4-...     -> /user/{id}</li>
     * </ul>
     */
    private String normalizePath(String uri) {
        if (uri == null) {
            return "unknown";
        }
        // 替换 UUID 格式片段
        String normalized = uri.replaceAll(
                "/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
                "/{id}");
        // 替换全数字 path 片段
        normalized = normalized.replaceAll("/\\d+", "/{id}");
        return normalized;
    }
}
