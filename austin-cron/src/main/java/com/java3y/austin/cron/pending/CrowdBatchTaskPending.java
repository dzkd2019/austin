package com.java3y.austin.cron.pending;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.text.StrPool;
import com.java3y.austin.cron.config.CronAsyncThreadPoolConfig;
import com.java3y.austin.cron.constants.PendingConstant;
import com.java3y.austin.support.vo.CrowdInfoVo;
import com.java3y.austin.service.api.domain.BatchSendRequest;
import com.java3y.austin.service.api.domain.MessageParam;
import com.java3y.austin.service.api.enums.BusinessCode;
import com.java3y.austin.service.api.service.SendService;
import com.java3y.austin.support.pending.AbstractLazyPending;
import com.java3y.austin.support.pending.PendingParam;
import com.java3y.austin.support.utils.RetryUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * 延迟批量处理人群信息
 * 调用 batch 发送接口 进行消息推送
 *
 * @author 3y
 */
@Slf4j
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CrowdBatchTaskPending extends AbstractLazyPending<CrowdInfoVo> {

    @Autowired
    private SendService sendService;

    public CrowdBatchTaskPending() {
        PendingParam<CrowdInfoVo> pendingParam = new PendingParam<>();
        pendingParam.setQueue(new LinkedBlockingQueue<>(PendingConstant.QUEUE_SIZE))
                .setTimeThreshold(PendingConstant.TIME_THRESHOLD)
                .setNumThreshold(PendingConstant.NUM_THRESHOLD)
                .setExecutorService(CronAsyncThreadPoolConfig.getConsumePendingThreadPool());
        this.pendingParam = pendingParam;
    }

    @Override
    public void doHandle(List<CrowdInfoVo> crowdInfoVos) {

        // 1 & 2. 聚合参数相同的 receiver 并直接组装成 MessageParam
        List<MessageParam> messageParams = crowdInfoVos.stream()
                // 按 params (Map) 分组，下游提取 receiver 并用逗号拼接
                .collect(Collectors.groupingBy(
                        CrowdInfoVo::getParams,
                        Collectors.mapping(CrowdInfoVo::getReceiver, Collectors.joining(StrPool.COMMA))
                ))
                // 将分组后的结果映射为 MessageParam 对象
                .entrySet().stream()
                .map(entry -> MessageParam.builder()
                        .variables(entry.getKey())
                        .receiver(entry.getValue())
                        .build()
                )
                .toList(); // (如果是 JDK 16+ 直接用 toList(), 否则 collect(Collectors.toList()))

        if (CollUtil.isEmpty(messageParams)) {
            return;
        }

        // 3. 调用批量发送接口发送消息
        BatchSendRequest batchSendRequest = BatchSendRequest.builder().code(BusinessCode.COMMON_SEND.getCode())
                .messageParamList(messageParams)
                .messageTemplateId(CollUtil.getFirst(crowdInfoVos.iterator()).getMessageTemplateId())
                .build();
        RetryUtils.executeWithRetry(3, 1000, () -> sendService.batchSend(batchSendRequest));
    }

}
