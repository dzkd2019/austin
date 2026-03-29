package com.java3y.austin.support.backpressure;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
@Slf4j
public class RemoteCircuitBreakerManager {
    private final ConcurrentMap<String, Boolean> remotePausedState = new ConcurrentHashMap<>();

    public void onMessage(String message) {
        try {
            String[] msg = message.split(":");
            String groupId = msg[0];
            String status = msg[1];

            if("pause".equals(status)) {
                log.warn("收到消费端报告，group: {} 压力过大，暂时停止消费", groupId);
                remotePausedState.put(groupId, true);
            }
            else if ("resume".equals(status)) {
                log.info("收到消费端报告，group: {} 压力恢复，继续消费", groupId);
                remotePausedState.put(groupId, false);
            }
        } catch (Exception e) {
            log.error("处理远程熔断消息失败，message: {}, error: {}", message, e.getMessage(), e);
        }
    }

    public boolean isPaused(String groupId) {
        return remotePausedState.getOrDefault(groupId, false);
    }
}
