package com.java3y.austin.service.api.impl.action.send;

import cn.hutool.core.collection.CollUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import com.java3y.austin.common.domain.SimpleTaskInfo;
import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.common.enums.RespStatusEnum;
import com.java3y.austin.common.exception.MessageTimeoutException;
import com.java3y.austin.common.exception.NetWorkTimeoutException;
import com.java3y.austin.common.exception.SystemBusyException;
import com.java3y.austin.common.pipeline.BusinessProcess;
import com.java3y.austin.common.pipeline.ProcessContext;
import com.java3y.austin.common.pipeline.ProcessException;
import com.java3y.austin.common.vo.BasicResultVO;
import com.java3y.austin.service.api.impl.domain.SendTaskModel;
import com.java3y.austin.support.constans.MdcConstant;
import com.java3y.austin.support.mq.MqRateLimiter;
import com.java3y.austin.support.mq.SendMqService;
import com.java3y.austin.support.utils.GroupIdMappingUtils;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author 3y
 * 1. 超时校验：消息在 Pending 队列等待超过阈值则丢弃，防止雪崩
 * 2. 全局限流：通过 {@link MqRateLimiter} 信号量保护，覆盖 API 与定时任务全路径
 * 3. 将消息发送到 MQ
 * 4. 返回拼装好的 messageId 给到接口调用方
 */
@Slf4j
@Service
@RefreshScope
public class SendMqAction implements BusinessProcess<SendTaskModel> {
    /**
     * 默认超时阈值（毫秒）
     */
    private static final long DEFAULT_PENDING_TIMEOUT_MS = 3000L;

    @Autowired
    private SendMqService sendMqService;

    @Value("${austin.business.tagId.value}")
    private String tagId;

    /**
     * 消息在队列中等待发往 MQ 的最大超时阈值（毫秒），可通过配置覆盖
     */
    @Value("${austin.mq.pending-timeout-ms:" + DEFAULT_PENDING_TIMEOUT_MS + "}")
    private long pendingTimeoutMs;

    @Override
    public void process(ProcessContext<SendTaskModel> context) {
        SendTaskModel sendTaskModel = context.getProcessModel();
        List<TaskInfo> taskInfo = sendTaskModel.getTaskInfo();
        String traceId = MDC.get(MdcConstant.MDC_TRACE_ID);

        // 1. 超时校验：同一批次所有 TaskInfo 在 SendAssembleAction 中同时组装，enqueueTime 一致；
        //    取首个元素作为代表进行超时判断即可。
        TaskInfo firstTaskInfo = CollUtil.getFirst(taskInfo.iterator());
        if (firstTaskInfo != null && firstTaskInfo.getEnqueueTime() > 0) {
            long waitMs = System.currentTimeMillis() - firstTaskInfo.getEnqueueTime();
            if (waitMs > pendingTimeoutMs) {
                log.warn("MQ发送前消息超时，追踪ID={}，等待时长={}毫秒，阈值={}毫秒",
                        traceId, waitMs, pendingTimeoutMs);
                context.setNeedBreak(true).setResponse(BasicResultVO.fail(RespStatusEnum.SYSTEM_TIMEOUT));
                throw new MessageTimeoutException("message timeout before mq send, traceId=%s, waitMs=%dms, threshold=%dms".formatted(traceId, waitMs, pendingTimeoutMs));
            }
        }

        try {
            String message = JSON.toJSONString(sendTaskModel.getTaskInfo(), JSONWriter.Feature.WriteClassName);
            if (firstTaskInfo == null) {
                throw new IllegalStateException("当通过groupId进行 topic 路由时，taskInfo为空。");
            }
            String groupId = GroupIdMappingUtils.getGroupIdByTaskInfo(firstTaskInfo);
            sendMqService.send(groupId, message, tagId);

            context.setResponse(BasicResultVO.success(taskInfo.stream()
                    .map(v -> SimpleTaskInfo.builder()
                            .traceId(v.getTraceId())
                            .messageId(v.getMessageId())
                            .bizId(v.getBizId())
                            .build())
                    .collect(Collectors.toList())));
        } catch (NetWorkTimeoutException | SystemBusyException e) {
            throw e;
        } catch (Exception e) {
            context.setNeedBreak(true).setResponse(BasicResultVO.fail(RespStatusEnum.SERVICE_ERROR));
            throw new ProcessException(context, e);
        }
    }

}
