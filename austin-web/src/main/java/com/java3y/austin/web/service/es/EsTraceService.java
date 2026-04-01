package com.java3y.austin.web.service.es;

import com.java3y.austin.common.domain.TraceInfo;

import java.util.List;

/**
 * ES链路查询接口
 */
public interface EsTraceService {

    /**
     * 通过 messageId 查询所有链路记录
     */
    List<TraceInfo> traceByMessageId(String messageId);

    /**
     * 通过 traceId 查询最新一条链路记录
     */
    List<TraceInfo> traceByTraceId(String traceId);

    /**
     * 通过 bizId 查询最新一条链路记录
     */
    List<TraceInfo> traceByBizId(String bizId);

    /**
     * 通过 templateId 查询最新一条链路记录
     */
    List<TraceInfo> traceByTemplateId(Long templateId);

    /**
     * 通过 receiver 查询最新一条链路记录
     */
    List<TraceInfo> traceByReceiver(String receiver);
}
