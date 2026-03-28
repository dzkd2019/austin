package com.java3y.austin.service.api.impl.action.send;

import cn.hutool.core.collection.CollUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import com.java3y.austin.common.domain.SimpleTaskInfo;
import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.common.enums.RespStatusEnum;
import com.java3y.austin.common.pipeline.BusinessProcess;
import com.java3y.austin.common.pipeline.ProcessContext;
import com.java3y.austin.common.vo.BasicResultVO;
import com.java3y.austin.service.api.impl.domain.SendTaskModel;
import com.java3y.austin.support.mq.MqRateLimiter;
import com.java3y.austin.support.mq.SendMqService;
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

    /** MDC 中存放 traceId 的 key，与 MdcEnrichFilter 保持一致 */
    private static final String MDC_TRACE_ID = "traceId";

    /** 默认超时阈值（毫秒） */
    private static final long DEFAULT_PENDING_TIMEOUT_MS = 3000L;

    @Autowired
    private SendMqService sendMqService;

    @Autowired
    private MqRateLimiter mqRateLimiter;

    @Value("${austin.business.topic.name}")
    private String sendMessageTopic;

    @Value("${austin.business.tagId.value}")
    private String tagId;

    @Value("${austin.mq.pipeline}")
    private String mqPipeline;

    /** 消息在队列中等待发往 MQ 的最大超时阈值（毫秒），可通过配置覆盖 */
    @Value("${austin.mq.pending-timeout-ms:" + DEFAULT_PENDING_TIMEOUT_MS + "}")
    private long pendingTimeoutMs;

    @Override
    public void process(ProcessContext<SendTaskModel> context) {
        SendTaskModel sendTaskModel = context.getProcessModel();
        List<TaskInfo> taskInfo = sendTaskModel.getTaskInfo();
        String traceId = MDC.get(MDC_TRACE_ID);

        // 1. 超时校验：同一批次所有 TaskInfo 在 SendAssembleAction 中同时组装，enqueueTime 一致；
        //    取首个元素作为代表进行超时判断即可。
        TaskInfo first = CollUtil.getFirst(taskInfo.iterator());
        if (first != null && first.getEnqueueTime() > 0) {
            long waitMs = System.currentTimeMillis() - first.getEnqueueTime();
            if (waitMs > pendingTimeoutMs) {
                log.warn("message timeout before mq send, traceId={}, waitMs={}ms, threshold={}ms",
                        traceId, waitMs, pendingTimeoutMs);
                context.setNeedBreak(true).setResponse(BasicResultVO.fail(RespStatusEnum.SYSTEM_TIMEOUT));
                return;
            }
        }

        // 2. 全局限流：非阻塞 tryAcquire 实现快速失败，避免主线程长时间阻塞。
        //    信号量许可耗尽时直接返回 SYSTEM_BUSY，不同于超时场景的 SYSTEM_TIMEOUT。
        boolean acquired = mqRateLimiter.getSemaphore().tryAcquire();
        try {
            if (!acquired) {
                log.warn("mq rate-limiter permits exhausted, traceId={}", traceId);
                context.setNeedBreak(true).setResponse(BasicResultVO.fail(RespStatusEnum.SYSTEM_BUSY));
                return;
            }

            String message = JSON.toJSONString(sendTaskModel.getTaskInfo(), JSONWriter.Feature.WriteClassName);
            sendMqService.send(sendMessageTopic, message, tagId);

            context.setResponse(BasicResultVO.success(taskInfo.stream()
                    .map(v -> SimpleTaskInfo.builder()
                            .businessId(v.getBusinessId())
                            .messageId(v.getMessageId())
                            .bizId(v.getBizId())
                            .build())
                    .collect(Collectors.toList())));
        } catch (Exception e) {
            context.setNeedBreak(true).setResponse(BasicResultVO.fail(RespStatusEnum.SERVICE_ERROR));
            log.error("send {} fail! traceId={}, params:{}", mqPipeline, traceId,
                    JSON.toJSONString(CollUtil.getFirst(taskInfo.listIterator())), e);
        } finally {
            // 无论成功、失败还是异常，已获取的许可必须在 finally 中归还，防止信号量泄露导致死锁
            if (acquired) {
                mqRateLimiter.getSemaphore().release();
            }
        }
    }

}

