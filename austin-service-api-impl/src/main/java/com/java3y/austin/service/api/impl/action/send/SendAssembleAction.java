package com.java3y.austin.service.api.impl.action.send;

import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.text.StrPool;
import cn.hutool.core.util.ReflectUtil;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.java3y.austin.common.constant.CommonConstant;
import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.common.dto.model.ContentModel;
import com.java3y.austin.common.enums.ChannelType;
import com.java3y.austin.common.enums.RespStatusEnum;
import com.java3y.austin.common.pipeline.BusinessProcess;
import com.java3y.austin.common.pipeline.ProcessContext;
import com.java3y.austin.common.pipeline.ProcessException;
import com.java3y.austin.common.vo.BasicResultVO;
import com.java3y.austin.service.api.domain.MessageParam;
import com.java3y.austin.service.api.impl.domain.SendTaskModel;
import com.java3y.austin.support.cache.MessageTemplateCaching;
import com.java3y.austin.support.constans.MdcConstant;
import com.java3y.austin.support.domain.MessageTemplate;
import com.java3y.austin.support.utils.ContentHolderUtil;
import com.java3y.austin.support.utils.TaskInfoUtils;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import java.lang.reflect.Field;
import java.util.*;

/**
 * @author 3y
 * @date 2021/11/22
 * @description 拼装参数
 */
@Slf4j
@Service
public class SendAssembleAction implements BusinessProcess<SendTaskModel> {

    private static final String LINK_NAME = "url";

    private final MessageTemplateCaching caching;

    public SendAssembleAction(MessageTemplateCaching caching) {
        this.caching = caching;
    }

    /**
     * 获取 contentModel，替换模板msgContent中占位符信息
     */
    private static ContentModel getContentModelValue(MessageTemplate messageTemplate, MessageParam messageParam) {

        // 得到真正的ContentModel 类型
        Integer sendChannel = messageTemplate.getSendChannel();
        Class<? extends ContentModel> contentModelClass = ChannelType.getChanelModelClassByCode(sendChannel);

        // 得到模板的 msgContent 和 入参
        Map<String, String> variables = messageParam.getVariables();
        JSONObject jsonObject = JSON.parseObject(messageTemplate.getMsgContent());


        // 通过反射 组装出 contentModel
        Field[] fields = ReflectUtil.getFields(contentModelClass);
        ContentModel contentModel = ReflectUtil.newInstance(contentModelClass);
        for (Field field : fields) {
            String originValue = jsonObject.getString(field.getName());

            if (CharSequenceUtil.isNotBlank(originValue)) {
                String resultValue = ContentHolderUtil.replacePlaceHolder(originValue, variables);
                Object resultObj = JSONUtil.isTypeJSONObject(resultValue) ? JSONUtil.toBean(resultValue, field.getType()) : resultValue;
                ReflectUtil.setFieldValue(contentModel, field, resultObj);
            }
        }

        // 如果 url 字段存在，则在url拼接对应的埋点参数
        String url = (String) ReflectUtil.getFieldValue(contentModel, LINK_NAME);
        if (CharSequenceUtil.isNotBlank(url)) {
            String resultUrl = TaskInfoUtils.generateUrl(url, messageTemplate.getId(), messageTemplate.getTemplateType());
            ReflectUtil.setFieldValue(contentModel, LINK_NAME, resultUrl);
        }
        return contentModel;
    }

    @Override
    public void process(ProcessContext<SendTaskModel> context) {
        SendTaskModel sendTaskModel = context.getProcessModel();
        Long messageTemplateId = sendTaskModel.getMessageTemplateId();

        try {
//            Optional<MessageTemplate> messageTemplate = messageTemplateDao.findById(messageTemplateId);
            Optional<MessageTemplate> messageTemplate = caching.getMessageTemplate(messageTemplateId);

            if (messageTemplate.isEmpty() || messageTemplate.get().getIsDeleted().equals(CommonConstant.TRUE)) {
                context.setNeedBreak(true).setResponse(BasicResultVO.fail(RespStatusEnum.TEMPLATE_NOT_FOUND));
                return;
            }
            List<TaskInfo> taskInfos = assembleTaskInfo(sendTaskModel, messageTemplate.get());
            sendTaskModel.setTaskInfo(taskInfos);
        } catch (Exception e) {
            context.setNeedBreak(true).setResponse(BasicResultVO.fail(RespStatusEnum.SERVICE_ERROR));
            throw new ProcessException("组装TaskInfo时发生错误", context, e);
        }

    }

    /**
     * 组装 TaskInfo 任务消息
     *
     */
    private List<TaskInfo> assembleTaskInfo(SendTaskModel sendTaskModel, MessageTemplate messageTemplate) {
        List<MessageParam> messageParamList = sendTaskModel.getMessageParamList();
        List<TaskInfo> taskInfoList = new ArrayList<>();

        String traceId = MDC.get(MdcConstant.MDC_TRACE_ID);
        String bizId = MDC.get(MdcConstant.MDC_BUSINESS_ID);

        for (MessageParam messageParam : messageParamList) {

            TaskInfo taskInfo = TaskInfo.builder()
                    .messageId(TaskInfoUtils.generateMessageId())
                    .bizId(bizId)
                    .messageTemplateId(messageTemplate.getId())
                    .traceId(traceId)
                    .receiver(new HashSet<>(Arrays.asList(messageParam.getReceiver().split(String.valueOf(StrPool.C_COMMA)))))
                    .idType(messageTemplate.getIdType())
                    .sendChannel(messageTemplate.getSendChannel())
                    .templateType(messageTemplate.getTemplateType())
                    .msgType(messageTemplate.getMsgType())
                    .shieldType(messageTemplate.getShieldType())
                    .sendAccount(messageTemplate.getSendAccount())
                    .enqueueTime(System.currentTimeMillis())
                    .contentModel(getContentModelValue(messageTemplate, messageParam)).build();

            if (CharSequenceUtil.isBlank(taskInfo.getBizId())) {
                taskInfo.setBizId(taskInfo.getMessageId());
            }

            taskInfoList.add(taskInfo);
        }

        return taskInfoList;

    }
}
