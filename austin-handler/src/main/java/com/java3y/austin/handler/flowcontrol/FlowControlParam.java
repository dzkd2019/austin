package com.java3y.austin.handler.flowcontrol;

import com.google.common.util.concurrent.RateLimiter;
import com.java3y.austin.handler.enums.RateLimitStrategy;
import com.java3y.austin.handler.flowcontrol.config.RateLimiterConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 3y
 * @date 2022/4/18
 * <p>
 * 流量控制所需要的参数
 */
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FlowControlParam {

    public RateLimiterConfig rateLimiterConfig;

    /**
     * 限流的策略
     * 子类初始化的时候指定
     */
    protected RateLimitStrategy rateLimitStrategy;
}
