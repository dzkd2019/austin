package com.java3y.austin.common.exception;

import com.java3y.austin.common.enums.RespStatusEnum;

/**
 * 消息超时异常
 *
 * <p>当消息在 Pending 队列中等待发往 MQ 的时间超过阈值，或获取全局限流许可超时时抛出此异常。
 * 全局异常处理器捕获后向调用方返回脱敏的超时提示，不暴露内部结构。
 *
 * @author mrawa
 */
public class MessageTimeoutException extends CommonException {

    public MessageTimeoutException(String message) {
        super(RespStatusEnum.SYSTEM_TIMEOUT.getCode(), message);
    }

    public MessageTimeoutException(String message, Exception cause) {
        super(RespStatusEnum.SYSTEM_TIMEOUT.getCode(), message, cause);
    }
}
