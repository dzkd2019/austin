package com.java3y.austin.service.api.impl.service;

import cn.monitor4all.logRecord.annotation.OperationLog;
import com.java3y.austin.common.domain.SimpleTaskInfo;
import com.java3y.austin.common.enums.RespStatusEnum;
import com.java3y.austin.common.exception.CommonException;
import com.java3y.austin.common.pipeline.ProcessContext;
import com.java3y.austin.common.pipeline.ProcessController;
import com.java3y.austin.common.vo.BasicResultVO;
import com.java3y.austin.service.api.domain.BatchSendRequest;
import com.java3y.austin.service.api.domain.SendRequest;
import com.java3y.austin.service.api.domain.SendResponse;
import com.java3y.austin.service.api.impl.domain.SendTaskModel;
import com.java3y.austin.service.api.service.SendService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

/**
 * 发送接口
 *
 * @author 3y
 */
@Service
@Slf4j
public class SendServiceImpl implements SendService {

    /**
     * batchSend 单次请求允许的最大 MessageParam 数量，防止超大批次导致 OOM 或处理超时
     */
    private static final int MAX_BATCH_SIZE = 1000;

    @Autowired
    @Qualifier("apiProcessController")
    private ProcessController processController;

    @Override
    @SuppressWarnings("unchecked")
    public SendResponse send(SendRequest sendRequest) {
        if (ObjectUtils.isEmpty(sendRequest)) {
            return new SendResponse(RespStatusEnum.CLIENT_BAD_PARAMETERS.getCode(), RespStatusEnum.CLIENT_BAD_PARAMETERS.getMsg(), null);
        }

        SendTaskModel sendTaskModel = SendTaskModel.builder()
                .messageTemplateId(sendRequest.getMessageTemplateId())
                .messageParamList(Collections.singletonList(sendRequest.getMessageParam()))
                .build();

        ProcessContext<SendTaskModel> context = ProcessContext.<SendTaskModel>builder()
                .code(sendRequest.getCode())
                .processModel(sendTaskModel)
                .needBreak(false)
                .response(BasicResultVO.success()).build();

        var process = processController.process(context);

        return new SendResponse(process.getResponse().getStatus(), process.getResponse().getMsg(), (List<SimpleTaskInfo>) process.getResponse().getData());
    }

    @SuppressWarnings("unchecked")
    @Override
    public SendResponse batchSend(BatchSendRequest batchSendRequest) {
        if (ObjectUtils.isEmpty(batchSendRequest)) {
            return new SendResponse(RespStatusEnum.CLIENT_BAD_PARAMETERS.getCode(), RespStatusEnum.CLIENT_BAD_PARAMETERS.getMsg(), null);
        }
        if(batchSendRequest.getMessageParamList() == null) {
            return new SendResponse(RespStatusEnum.CLIENT_BAD_PARAMETERS.getCode(), "批量发送至少需要一条消息参数", null);
        }
        if (batchSendRequest.getMessageParamList().size() > MAX_BATCH_SIZE) {
            return new SendResponse(RespStatusEnum.CLIENT_BAD_PARAMETERS.getCode(),
                    "批量发送每次最多支持 " + MAX_BATCH_SIZE + " 条，当前: " + batchSendRequest.getMessageParamList().size(), null);
        }

        Long templateId = batchSendRequest.getMessageTemplateId();
        int batchSize = batchSendRequest.getMessageParamList().size();
        log.info("处理批量发送请求，模板ID: {}, 批次大小: {}", templateId, batchSize);

        SendTaskModel sendTaskModel = SendTaskModel.builder()
                .messageTemplateId(templateId)
                .messageParamList(batchSendRequest.getMessageParamList())
                .build();

        ProcessContext<SendTaskModel> context = ProcessContext.<SendTaskModel>builder()
                .code(batchSendRequest.getCode())
                .processModel(sendTaskModel)
                .needBreak(false)
                .response(BasicResultVO.success()).build();

        var process = processController.process(context);
        var response = process.getResponse();

        if (!"0".equals(response.getStatus())) {
            RespStatusEnum code = RespStatusEnum.getByCode(response.getStatus());
            if (code == null) {
                throw new CommonException(response.getStatus(), response.getMsg());
            }
            throw new CommonException(code);
        }

        return new SendResponse(process.getResponse().getStatus(), process.getResponse().getMsg(), (List<SimpleTaskInfo>) process.getResponse().getData());
    }


}
