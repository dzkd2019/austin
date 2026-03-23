package com.java3y.austin.handler.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

/**
 * 限流枚举
 *
 * @author 3y
 */
@Getter
@ToString
@AllArgsConstructor
public enum RateLimitStrategy {

    NONE(0, "不限流"),

    /**
     *  基于令牌桶限流
     */
    TOKEN_BUCKET_RATE_LIMIT(30, "基于令牌桶限流")
    ;

    private final Integer code;
    private final String description;


}
