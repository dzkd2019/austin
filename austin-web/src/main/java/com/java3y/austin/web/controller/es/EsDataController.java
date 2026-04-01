package com.java3y.austin.web.controller.es;

import com.java3y.austin.common.domain.TraceInfo;
import com.java3y.austin.web.service.es.EsTraceService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * ES链路查询接口
 */
@RestController
@RequestMapping("/es/trace")
@Tag(name = "ES链路查询接口")
public class EsDataController {

    private final EsTraceService esTraceService;

    public EsDataController(EsTraceService esTraceService) {
        this.esTraceService = esTraceService;
    }

    @GetMapping("/message")
    @Operation(summary = "通过messageId查询所有链路记录")
    public List<TraceInfo> traceByMessageId(@RequestParam("messageId") String messageId) {
        return esTraceService.traceByMessageId(messageId);
    }

    @GetMapping("/traceId")
    @Operation(summary = "通过traceId查询最新一条链路记录")
    public List<TraceInfo> traceByTraceId(@RequestParam("traceId") String traceId) {
        return esTraceService.traceByTraceId(traceId);
    }

    @GetMapping("/bizId")
    @Operation(summary = "通过bizId查询最新一条链路记录")
    public List<TraceInfo> traceByBizId(@RequestParam("bizId") String bizId) {
        return esTraceService.traceByBizId(bizId);
    }

    @GetMapping("/templateId")
    @Operation(summary = "通过templateId查询最新一条链路记录")
    public List<TraceInfo> traceByTemplateId(@RequestParam("templateId") Long templateId) {
        return esTraceService.traceByTemplateId(templateId);
    }

    @GetMapping("/receiver")
    @Operation(summary = "通过receiver查询最新一条链路记录")
    public List<TraceInfo> traceByReceiver(@RequestParam("receiver") String receiver) {
        return esTraceService.traceByReceiver(receiver);
    }
}
