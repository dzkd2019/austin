package com.java3y.austin.common.exception;

import com.java3y.austin.common.enums.RespStatusEnum;

/**
 * 向 Redis 发送请求失败时抛出该异常
 *
 * @author: mrawa
 */
public class RedisOperationException extends CommonException {
    public RedisOperationException(String message) {
        super(RespStatusEnum.REDIS_ERROR.getCode(), message);
    }

    public RedisOperationException(String message, Exception cause) {
        super(RespStatusEnum.REDIS_ERROR.getCode(), message, cause);
    }
}
