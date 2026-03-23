package com.java3y.austin.handler.flowcontrol.controller;

import com.java3y.austin.handler.flowcontrol.config.RateLimiterConfig;

public interface FlowController {
    double acquire() throws InterruptedException;

    void updateConfig(RateLimiterConfig config);
}
