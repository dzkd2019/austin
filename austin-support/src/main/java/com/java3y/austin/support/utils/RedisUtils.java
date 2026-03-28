package com.java3y.austin.support.utils;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.IdUtil;
import com.google.common.base.Throwables;
import com.java3y.austin.common.constant.CommonConstant;
import com.java3y.austin.common.exception.RedisOperationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author 3y
 * @date 2021/12/10
 * 对Redis的某些操作二次封装
 */
@Component
@Slf4j
public class RedisUtils {


    private final StringRedisTemplate redisTemplate;

    public RedisUtils(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * mGet将结果封装为Map
     *
     **/
    public Map<String, String> mGet(List<String> keys) {
        HashMap<String, String> result = new HashMap<>(keys.size());
        try {
            List<String> value = redisTemplate.opsForValue().multiGet(keys);
            if (CollUtil.isNotEmpty(value)) {
                for (int i = 0; i < keys.size(); i++) {
                    if (Objects.nonNull(value.get(i))) {
                        result.put(keys.get(i), value.get(i));
                    }

                }
            }
        } catch (Exception e) {
            String errKeys = String.join(",", keys);
            throw new RedisOperationException("通过 mget 读取Redis失败, keys: " + errKeys, e);
        }
        return result;
    }


    public Map<Object, Object> hGetAll(String key) {
        try {
            return redisTemplate.opsForHash().entries(key);
        } catch (Exception e) {
            throw new RedisOperationException("通过 hGetAll 读取Redis失败, key: " + key, e);
        }
    }

    /**
     * lRange
     *
     */
    public List<String> lRange(String key, long start, long end) {
        try {
            return redisTemplate.opsForList().range(key, start, end);
        } catch (Exception e) {
            throw new RedisOperationException("通过 lRange 读取Redis失败, range: " + start + " - " + end, e);
        }
    }

    /**
     * pipeline 设置 key-value 并设置过期时间
     */
    public void pipelineSetEx(Map<String, String> keyValues, Long seconds) {
        try {
            redisTemplate.executePipelined((RedisCallback<String>) connection -> {
                for (Map.Entry<String, String> entry : keyValues.entrySet()) {
                    connection.stringCommands()
                            .setEx(entry.getKey().getBytes(StandardCharsets.UTF_8), seconds,
                                    entry.getValue().getBytes(StandardCharsets.UTF_8));
                }
                return null;
            });
        } catch (Exception e) {
            String errKeys = String.join(",", keyValues.keySet());
            throw new RedisOperationException("pipeline向Redis写入失败, keys: " + errKeys, e);
        }
    }


    /**
     * lpush 方法 并指定 过期时间
     */
    public void lPush(String key, String value, Long seconds) {
        try {
            redisTemplate.executePipelined((RedisCallback<String>) connection -> {
                connection.listCommands().lPush(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
                connection.keyCommands().expire(key.getBytes(StandardCharsets.UTF_8), seconds);
                return null;
            });
        } catch (Exception e) {
            throw new RedisOperationException("通过 lPush 向Redis写入失败, key: " + key, e);
        }
    }

    /**
     * lLen 方法
     */
    public Long lLen(String key) {
        try {
            return redisTemplate.opsForList().size(key);
        } catch (Exception e) {
            throw new RedisOperationException("通过 lLen 读取Redis失败, key: " + key, e);
        }
//        return 0L;
    }

    /**
     * lPop 方法
     */
    public String lPop(String key) {
        try {
            return redisTemplate.opsForList().leftPop(key);
        } catch (Exception e) {
            throw new RedisOperationException("通过 lPop 从Redis读取失败, key: " + key, e);
        }
    }

    /**
     * pipeline 设置 key-value 并设置过期时间
     *
     * @param seconds 过期时间
     * @param delta   自增的步长
     */
    public void pipelineHashIncrByEx(Map<String, String> keyValues, Long seconds, Long delta) {
        try {
            redisTemplate.executePipelined((RedisCallback<String>) connection -> {
                for (Map.Entry<String, String> entry : keyValues.entrySet()) {
                    connection.hashCommands().hIncrBy(entry.getKey().getBytes(StandardCharsets.UTF_8),
                            entry.getValue().getBytes(StandardCharsets.UTF_8),
                            delta);
                    connection.keyCommands().expire(entry.getKey().getBytes(StandardCharsets.UTF_8),
                            seconds);
                }
                return null;
            });
        } catch (Exception e) {
            String errKeys = String.join(",", keyValues.keySet());
            throw new RedisOperationException("pipeline向Redis写入失败, keys: " + errKeys, e);
        }
    }

    /**
     * 执行指定的lua脚本返回执行结果
     * --KEYS[1]: 限流 key
     * --ARGV[1]: 限流窗口
     * --ARGV[2]: 当前时间戳（作为score）
     * --ARGV[3]: 阈值
     * --ARGV[4]: score 对应的唯一value
     *
     */
    public Boolean execLimitLua(RedisScript<Long> redisScript, List<String> keys, String... args) {

        // 可变参数转数组
        String[] argsArray = args != null ? args : new String[0];
        try {
            Long execute = redisTemplate.execute(redisScript, keys, (Object[]) argsArray);
            if (Objects.isNull(execute)) {
                return true;
            }
            return CommonConstant.TRUE.equals(execute.intValue());
        } catch (Exception e) {
            log.error("执行lua脚本时失败", e);
        }
        return false;
    }


    public List<Boolean> execLimitLuaPipeline(RedisScript<Long> redisScript, List<String> keys, Long windowSize, Integer threshold) {
        if (keys == null || keys.isEmpty()) {
            return new ArrayList<>();
        }

        String script = redisScript.getScriptAsString();
        RedisSerializer<String> serializer = redisTemplate.getStringSerializer();
        final long currentTime = System.currentTimeMillis();
        byte[] windowBytes = serializer.serialize(String.valueOf(windowSize));
        byte[] thresholdBytes = serializer.serialize(String.valueOf(threshold));
        byte[] scoreBytes = serializer.serialize(String.valueOf(currentTime));
        byte[] scriptBytes = serializer.serialize(script);

        List<Object> pipelineResults = redisTemplate.executePipelined((RedisCallback<Long>) connection -> {
            for (String key : keys) {
                byte[] keyBytes = serializer.serialize(key);
                byte[] valueBytes = serializer.serialize(String.valueOf(IdUtil.getSnowflake().nextId()));

                connection.scriptingCommands().eval(scriptBytes, ReturnType.INTEGER, 1, keyBytes, windowBytes, scoreBytes, thresholdBytes, valueBytes);
            }
            return null;
        });

        List<Boolean> results = new ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            Object result = pipelineResults.get(i);
            if (result == null) {
                // Redis 执行失败，保守策略：认为需要去重（过滤掉）
                results.add(true);
                log.warn("Pipeline 执行返回 null，key: {}", keys.get(i));
            } else if (result instanceof Long) {
                results.add(CommonConstant.TRUE.equals(((Long) result).intValue()));
            } else {
                // 异常情况，记录日志
                log.error("Pipeline 返回类型异常，key: {}, type: {}", keys.get(i), result.getClass());
                results.add(true); // 保守策略
            }
        }

        return results;
    }
}
