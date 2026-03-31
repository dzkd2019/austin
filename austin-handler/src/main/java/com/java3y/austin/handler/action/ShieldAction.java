package com.java3y.austin.handler.action;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import com.java3y.austin.common.domain.AnchorInfo;
import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.common.domain.TraceInfo;
import com.java3y.austin.common.enums.AnchorState;
import com.java3y.austin.common.enums.RespStatusEnum;
import com.java3y.austin.common.enums.ShieldType;
import com.java3y.austin.common.pipeline.BusinessProcess;
import com.java3y.austin.common.pipeline.ProcessContext;
import com.java3y.austin.common.vo.BasicResultVO;
import com.java3y.austin.support.utils.LogUtils;
import com.java3y.austin.support.utils.RedisUtils;
import com.java3y.austin.support.utils.TraceUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;


/**
 * 屏蔽消息
 * 1. 当接收到该消息是夜间，直接屏蔽（不发送）
 * 2. 当接收到该消息是夜间，次日9点发送
 * example:当消息下发至austin平台时，已经是凌晨1点，业务希望此类消息在次日的早上9点推送
 * (配合 分布式任务定时任务框架搞掂)
 *
 * @author 3y
 */
@Service
@Slf4j
public class ShieldAction implements BusinessProcess<TaskInfo> {

    private static final String NIGHT_SHIELD_BUT_NEXT_DAY_SEND_KEY = "night_shield_send";
    private static final long SECONDS_OF_A_DAY = 86400L;

    /**
     * 默认早上8点之前是凌晨
     */
    private static final int NIGHT = 8;

    private final RedisUtils redisUtils;

    private final TraceUtils traceUtils;

    public ShieldAction(RedisUtils redisUtils, TraceUtils traceUtils) {
        this.redisUtils = redisUtils;
        this.traceUtils = traceUtils;
    }

    @Override
    public void process(ProcessContext<TaskInfo> context) {
        TaskInfo taskInfo = context.getProcessModel();

        if (ShieldType.NIGHT_NO_SHIELD.getCode().equals(taskInfo.getShieldType())) {
            return;
        }

        if (LocalDateTime.now().getHour() < NIGHT) {
            if (ShieldType.NIGHT_SHIELD.getCode().equals(taskInfo.getShieldType())) {
                traceUtils.trace(new TraceInfo(taskInfo, AnchorState.NIGHT_SHIELD));
                log.warn("消息被夜间拦截规则阻拦, messageId: {}, receivers: {}", taskInfo.getMessageId(), String.join(",", taskInfo.getReceiver()));
                context.setResponse(BasicResultVO.fail(RespStatusEnum.MESSAGE_IS_SHIELD));
            }
            if (ShieldType.NIGHT_SHIELD_BUT_NEXT_DAY_SEND.getCode().equals(taskInfo.getShieldType())) {
                redisUtils.lPush(NIGHT_SHIELD_BUT_NEXT_DAY_SEND_KEY, JSON.toJSONString(taskInfo,
                                JSONWriter.Feature.WriteClassName),
                        SECONDS_OF_A_DAY);
                traceUtils.trace(new TraceInfo(taskInfo, AnchorState.NIGHT_SHIELD_NEXT_SEND));
                log.warn("消息被夜间拦截，第二天再发送, messageId: {}, receivers: {}", taskInfo.getMessageId(), String.join(",", taskInfo.getReceiver()));
                context.setResponse(BasicResultVO.fail(RespStatusEnum.MESSAGE_IS_SHIELD_NEXT_SEND));
            }
            context.setNeedBreak(true);
        }

    }
}
