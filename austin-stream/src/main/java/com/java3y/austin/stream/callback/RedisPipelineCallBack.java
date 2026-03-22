package com.java3y.austin.stream.callback;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.util.List;

/**
 * redis pipeline接口定义
 *
 * @author 3y
 */
public interface RedisPipelineCallBack<K, V> {

    /**
     * 具体执行逻辑
     *
     * @param redisAsyncCommands
     * @return
     */
    List<RedisFuture<?>> invoke(RedisAsyncCommands<K, V> redisAsyncCommands);

}
