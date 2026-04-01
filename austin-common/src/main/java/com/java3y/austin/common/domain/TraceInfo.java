package com.java3y.austin.common.domain;

import com.java3y.austin.common.enums.AnchorState;

import java.util.Set;

public record TraceInfo(String bizId, String traceId, String messageId, Long templateId, AnchorState state,
                        Set<String> receivers, long timeStamp) {
    public TraceInfo(TaskInfo taskInfo, AnchorState state) {
        this(taskInfo.getBizId(), taskInfo.getTraceId(), taskInfo.getMessageId(), taskInfo.getMessageTemplateId(), state, taskInfo.getReceiver(), System.currentTimeMillis());
    }

    public TraceInfo(TaskInfo taskInfo, AnchorState state, Set<String> receivers) {
        this(taskInfo.getBizId(), taskInfo.getTraceId(), taskInfo.getMessageId(), taskInfo.getMessageTemplateId(), state, receivers, System.currentTimeMillis());
    }
}
