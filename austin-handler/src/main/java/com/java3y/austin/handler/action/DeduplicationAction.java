package com.java3y.austin.handler.action;

import cn.hutool.core.collection.CollUtil;
import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.common.enums.AnchorState;
import com.java3y.austin.common.enums.RespStatusEnum;
import com.java3y.austin.common.pipeline.BusinessProcess;
import com.java3y.austin.common.pipeline.ProcessContext;
import com.java3y.austin.common.vo.BasicResultVO;
import com.java3y.austin.handler.config.AustinMessageSendProperties;
import com.java3y.austin.handler.deduplication.DeduplicationHolder;
import com.java3y.austin.handler.deduplication.DeduplicationParam;
import org.springframework.stereotype.Service;


/**
 * 去重服务
 * 1. 根据相同内容N分钟去重（SlideWindowLimitService）
 * 2. 相同的渠道一天内频次去重（SimpleLimitService）
 *
 * @author 3y
 */
@Service
public class DeduplicationAction implements BusinessProcess<TaskInfo> {
    private final DeduplicationHolder deduplicationHolder;

    private final AustinMessageSendProperties austinMessageSendProperties;

    public DeduplicationAction(DeduplicationHolder deduplicationHolder, AustinMessageSendProperties austinMessageSendProperties) {
        this.deduplicationHolder = deduplicationHolder;
        this.austinMessageSendProperties = austinMessageSendProperties;
    }

    @Override
    public void process(ProcessContext<TaskInfo> context) {
        TaskInfo taskInfo = context.getProcessModel();

        var rule = austinMessageSendProperties.getDeduplicationRule();

        for (var entry : rule.entrySet()) {
            var config = entry.getValue();
            var type = entry.getKey();
            DeduplicationParam param = switch (type) {
                case FREQUENCY -> DeduplicationParam.builder().
                        taskInfo(taskInfo).countNum(config.num()).anchorState(AnchorState.RULE_DEDUPLICATION).build();

                case CONTENT ->
                        DeduplicationParam.builder().taskInfo(taskInfo).deduplicationTime(config.time()).countNum(config.num())
                                .anchorState(AnchorState.CONTENT_DEDUPLICATION).build();
            };
            if (param != null)
                deduplicationHolder.selectService(type).deduplication(param);
        }

        if (CollUtil.isEmpty(taskInfo.getReceiver())) {
            context.setNeedBreak(true);
            context.setResponse(BasicResultVO.fail(RespStatusEnum.MESSAGE_IS_DEDUPLICATION));
        }
    }
}
