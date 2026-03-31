package com.java3y.austin.web.filter;

import cn.hutool.core.util.IdUtil;
import com.java3y.austin.support.constans.MdcConstant;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * HTTP 请求 MDC 字段注入过滤器
 *
 * <p>为每个 HTTP 请求向 SLF4J MDC 注入以下结构化字段，供 Graylog 聚合分析：
 * <ul>
 *   <li>{@code traceId}    – 唯一请求追踪 ID（优先取请求头 X-Trace-Id，否则自动生成）</li>
 *   <li>{@code path}       – 请求 URI 路径</li>
 *   <li>{@code userId}     – 用户标识（取请求头 X-User-Id）</li>
 *   <li>{@code requestIp}  – 客户端 IP（支持反向代理透传）</li>
 * </ul>
 * 在请求结束后清理 MDC，防止线程池复用导致的 MDC 污染。
 * <p>该过滤器优先级最高（{@link Ordered#HIGHEST_PRECEDENCE}），确保后续所有过滤器和
 * Controller 均能使用 MDC 字段。
 *
 * @author 3y
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class MdcEnrichFilter extends OncePerRequestFilter {

    public static final String MDC_USER_ID = "userId";

    private static final String HEADER_TRACE_ID = "X-Trace-Id";
    private static final String HEADER_USER_ID = "X-User-Id";
    private static final String HEADER_FORWARDED_FOR = "X-Forwarded-For";

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        try {
            // 1. traceId：优先取上游传入的 header，否则本地生成
            String traceId = request.getHeader(HEADER_TRACE_ID);
            if (traceId == null || traceId.isBlank()) {
                traceId = IdUtil.fastSimpleUUID();
            }
            MDC.put(MdcConstant.MDC_TRACE_ID, traceId);
            MDC.put(MdcConstant.MDC_BUSINESS_ID, traceId);

            // 2. 请求路径
            MDC.put(MdcConstant.MDC_PATH, request.getRequestURI());

            // 3. 用户标识（业务层从 Token 解析后放入 header，此处透传）
            String userId = request.getHeader(HEADER_USER_ID);
            if (userId != null && !userId.isBlank()) {
                MDC.put(MDC_USER_ID, userId);
            }

            // 4. 客户端 IP（支持反向代理）
            String ip = request.getHeader(HEADER_FORWARDED_FOR);
            if (ip != null && !ip.isBlank()) {
                // X-Forwarded-For 可能包含多个 IP，取第一个
                int commaIdx = ip.indexOf(',');
                ip = commaIdx > 0 ? ip.substring(0, commaIdx).trim() : ip.trim();
            } else {
                ip = request.getRemoteAddr();
            }
            MDC.put(MdcConstant.MDC_REQUEST_IP, ip);

            // 将 traceId 写回响应头，方便前端日志关联
            response.setHeader(HEADER_TRACE_ID, traceId);

            filterChain.doFilter(request, response);
        } finally {
            // 【关键】必须在 finally 中清理 MDC，防止虚拟线程/线程池复用时 MDC 污染
            MDC.remove(MdcConstant.MDC_TRACE_ID);
            MDC.remove(MdcConstant.MDC_BUSINESS_ID);
            MDC.remove(MDC_USER_ID);
            MDC.remove(MdcConstant.MDC_PATH);
            MDC.remove(MdcConstant.MDC_REQUEST_IP);
        }
    }
}
