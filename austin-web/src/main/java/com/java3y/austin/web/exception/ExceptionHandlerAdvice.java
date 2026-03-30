package com.java3y.austin.web.exception;

import com.java3y.austin.common.enums.RespStatusEnum;
import com.java3y.austin.common.exception.CommonException;
import com.java3y.austin.common.exception.MessageTimeoutException;
import com.java3y.austin.common.exception.RedisOperationException;
import com.java3y.austin.common.exception.SystemBusyException;
import com.java3y.austin.common.vo.BasicResultVO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * 全局异常统一处理
 *
 * <p>后端完整记录异常堆栈与 traceId，前端仅返回脱敏的安全提示，不暴露内部结构。
 *
 * @author kl / 3y
 */
@ControllerAdvice(basePackages = "com.java3y.austin.web.controller")
@ResponseBody
public class ExceptionHandlerAdvice {

    private static final Logger log = LoggerFactory.getLogger(ExceptionHandlerAdvice.class);

    /** 与 MdcEnrichFilter.MDC_TRACE_ID 保持一致 */
    private static final String MDC_TRACE_ID = "traceId";

    /**
     * 消息超时 / 限流快速失败：系统超时，请稍后再试
     */
    @ExceptionHandler(MessageTimeoutException.class)
    @ResponseStatus(HttpStatus.REQUEST_TIMEOUT)
    public BasicResultVO<String> handleMessageTimeout(MessageTimeoutException e) {
        log.error("消息在发送到 MQ 之前超时", e);
        return BasicResultVO.fail(RespStatusEnum.SYSTEM_TIMEOUT);
    }

    @ExceptionHandler(SystemBusyException.class)
    @ResponseStatus(HttpStatus.TOO_MANY_REQUESTS)
    public BasicResultVO<String> handleSystemBusyException(SystemBusyException e) {
        log.error("消息在发送到 MQ 之前获取 Semaphore 失败，系统繁忙", e);
        return BasicResultVO.fail(RespStatusEnum.SYSTEM_BUSY);
    }

    /**
     * 业务异常：返回业务状态码与脱敏信息
     */
    @ExceptionHandler(CommonException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public BasicResultVO<RespStatusEnum> handleCommonException(CommonException e) {
        log.error("业务处理中发生异常", e);
        return new BasicResultVO<>(e.getCode(), e.getMessage(), e.getRespStatusEnum());
    }

    /**
     * 兜底异常：记录完整堆栈，返回通用系统错误提示
     */
    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public BasicResultVO<String> handleException(Exception e) {
        log.error("未处理的异常", e);
        return BasicResultVO.fail(RespStatusEnum.ERROR_500);
    }

    @ExceptionHandler(RedisOperationException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public BasicResultVO<String> handleRedisOperationException(RedisOperationException e) {
        log.error("RedisOperationException, traceId={}", MDC.get(MDC_TRACE_ID), e);
        return BasicResultVO.fail(RespStatusEnum.ERROR_500);
    }
}


