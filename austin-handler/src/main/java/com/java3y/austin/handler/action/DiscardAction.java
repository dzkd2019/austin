package com.java3y.austin.handler.action;

import com.java3y.austin.common.domain.AnchorInfo;
import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.common.enums.AnchorState;
import com.java3y.austin.common.enums.RespStatusEnum;
import com.java3y.austin.common.pipeline.BusinessProcess;
import com.java3y.austin.common.pipeline.ProcessContext;
import com.java3y.austin.common.vo.BasicResultVO;
import com.java3y.austin.handler.config.AustinMessageSendProperties;
import com.java3y.austin.support.utils.LogUtils;
import org.springframework.stereotype.Service;

import java.util.List;


/**
 * 丢弃消息
 * 一般将需要丢弃的模板id写在分布式配置中心
 *
 * @author 3y
 */
@Service
public class DiscardAction implements BusinessProcess<TaskInfo> {

    private final LogUtils logUtils;

    private final AustinMessageSendProperties messageSendProperties;

    public DiscardAction(LogUtils logUtils, AustinMessageSendProperties messageSendProperties) {
        this.logUtils = logUtils;
        this.messageSendProperties = messageSendProperties;
    }

    @Override
    public void process(ProcessContext<TaskInfo> context) {
        TaskInfo taskInfo = context.getProcessModel();

        List<Long> discardTemplateIds = messageSendProperties.getDiscardMsgIds();
        if (discardTemplateIds.contains(taskInfo.getMessageTemplateId())) {
            logUtils.print(AnchorInfo.builder().bizId(taskInfo.getBizId()).messageId(taskInfo.getMessageId()).businessId(taskInfo.getBusinessId()).ids(taskInfo.getReceiver()).state(AnchorState.DISCARD.getCode()).build());
            context.setNeedBreak(true);
            context.setResponse(BasicResultVO.fail(RespStatusEnum.MESSAGE_IS_DISCARDED));
        }
    }
}
