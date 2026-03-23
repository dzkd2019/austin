package com.java3y.austin.handler.flowcontrol.config;

public record TokenBucketRateLimiterConfig(double qps, double burst) implements RateLimiterConfig {
}
