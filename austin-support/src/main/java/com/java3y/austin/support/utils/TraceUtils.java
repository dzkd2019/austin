package com.java3y.austin.support.utils;

import com.alibaba.fastjson2.JSON;
import com.java3y.austin.common.domain.TraceInfo;
import com.java3y.austin.support.mq.SendMqService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TraceUtils {
    private final SendMqService sendMqService;

    @Value("${austin.business.trace.topic.name}")
    private String topic;

    public TraceUtils(SendMqService sendMqService) {
        this.sendMqService = sendMqService;
    }

    public void trace(TraceInfo info) {
        sendMqService.asyncSend(topic, JSON.toJSONString(info));
    }
}
