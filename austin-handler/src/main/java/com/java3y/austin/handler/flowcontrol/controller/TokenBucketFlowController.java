package com.java3y.austin.handler.flowcontrol.controller;

import com.java3y.austin.handler.flowcontrol.config.RateLimiterConfig;
import com.java3y.austin.handler.flowcontrol.config.TokenBucketRateLimiterConfig;
import com.java3y.austin.handler.flowcontrol.ratelimiter.TokenBucketRateLimiter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TokenBucketFlowController implements FlowController {
    private final TokenBucketRateLimiter rateLimiter;
    private volatile RateLimiterConfig currentConfig;

    public TokenBucketFlowController(RateLimiterConfig rateLimiterConfig) {
        this.rateLimiter = new TokenBucketRateLimiter(rateLimiterConfig);
    }

    @Override
    public double acquire() throws InterruptedException {
        return rateLimiter.acquire();
    }

    @Override
    public void updateConfig(RateLimiterConfig config) {
        if(config == null) {
            return;
        }
        if(!(config instanceof TokenBucketRateLimiterConfig)) {
            throw new IllegalArgumentException("config must be instance of TokenBucketRateLimiterConfig");
        }

        if(currentConfig.equals(config)) {
            return;
        }

        log.info("令牌桶限流参数更新: {} -> {}", currentConfig, config);
        this.currentConfig = config;
        rateLimiter.setRate((TokenBucketRateLimiterConfig) config);
    }
}
