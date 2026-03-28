package com.java3y.austin.handler.handler;

import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.handler.flowcontrol.FlowControlFactory;
import com.java3y.austin.handler.flowcontrol.FlowControlParam;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.Objects;

/**
 * @author 3y
 * 发送各个渠道的handler
 */
@Slf4j
public abstract class BaseHandler implements Handler {
    /**
     * 标识渠道的Code
     * 子类初始化的时候指定
     */
    protected Integer channelCode;
    /**
     * 限流相关的参数
     * 子类初始化的时候指定
     */
    protected FlowControlParam flowControlParam;
    @Autowired
    private HandlerHolder handlerHolder;
    @Autowired
    private FlowControlFactory flowControlFactory;
    @Autowired
    private StringRedisTemplate redisTemplate;

    /**
     * 初始化渠道与Handler的映射关系
     */
    @PostConstruct
    private void init() {
        handlerHolder.putHandler(channelCode, this);
    }


    @Override
    public void handle(TaskInfo taskInfo) {
        // 只有子类指定了限流参数，才需要限流
        if (Objects.nonNull(flowControlParam)) {
            flowControlFactory.flowControl(taskInfo, flowControlParam);
        }
        if (doHandle(taskInfo)) {
//            logUtils.print(AnchorInfo.builder().state(AnchorState.SEND_SUCCESS.getCode()).bizId(taskInfo.getBizId()).messageId(taskInfo.getMessageId()).businessId(taskInfo.getBusinessId()).ids(taskInfo.getReceiver()).build());
            log.info("调用接口发送成功");
            return;
        }
//        logUtils.print(AnchorInfo.builder().state(AnchorState.SEND_FAIL.getCode()).bizId(taskInfo.getBizId()).messageId(taskInfo.getMessageId()).businessId(taskInfo.getBusinessId()).ids(taskInfo.getReceiver()).build());
    }


    /**
     * 统一处理的handler接口
     *
     * @param taskInfo
     * @return
     */
    public abstract boolean doHandle(TaskInfo taskInfo);


    /**
     * 将撤回的消息存储到redis（使用 pipeline 将 4 次网络往返合并为 1 次，降低 Redis 延迟）
     *
     * @param prefix            redis前缀
     * @param messageTemplateId 消息模板id
     * @param taskId            消息下发taskId
     * @param expireTime        存储到redis的有效时间（跟对应渠道可撤回多久的消息有关系)
     */
    protected void saveRecallInfo(String prefix, Long messageTemplateId, String taskId, Long expireTime) {
        RedisSerializer<String> serializer = redisTemplate.getStringSerializer();
        byte[] templateKey = serializer.serialize(prefix + messageTemplateId);
        byte[] taskKey = serializer.serialize(prefix + taskId);
        byte[] taskIdBytes = serializer.serialize(taskId);

        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            connection.listCommands().lPush(templateKey, taskIdBytes);
            connection.stringCommands().set(taskKey, taskIdBytes);
            connection.keyCommands().expire(templateKey, expireTime);
            connection.keyCommands().expire(taskKey, expireTime);
            return null;
        });
    }


}
