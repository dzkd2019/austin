package com.java3y.austin.handler.handler.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.text.CharSequenceUtil;
import com.java3y.austin.common.domain.RecallTaskInfo;
import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.common.dto.account.sms.SmsAccount;
import com.java3y.austin.common.dto.model.SmsContentModel;
import com.java3y.austin.common.enums.ChannelType;
import com.java3y.austin.handler.config.AustinMessageSendProperties;
import com.java3y.austin.handler.domain.sms.MessageTypeSmsConfig;
import com.java3y.austin.handler.domain.sms.SmsParam;
import com.java3y.austin.handler.enums.LoadBalancerStrategy;
import com.java3y.austin.handler.handler.BaseHandler;
import com.java3y.austin.handler.loadbalance.ServiceLoadBalancerFactory;
import com.java3y.austin.handler.script.SmsScript;
import com.java3y.austin.support.dao.SmsRecordDao;
import com.java3y.austin.support.domain.SmsRecord;
import com.java3y.austin.support.utils.AccountUtils;
import com.java3y.austin.support.utils.RetryUtils;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 短信发送处理
 *
 * @author 3y
 */
@Component
@Slf4j
public class SmsHandler extends BaseHandler implements ApplicationContextAware {

    /**
     * 流量自动分配策略
     */
    private static final Integer AUTO_FLOW_RULE = 0;
    private ApplicationContext applicationContext;


    /**
     * 默认负载均衡为随机加权, 待拓展读取配置, 不同Handler可绑定不同的负载均衡策略
     */
    private static final String loadBalancerStrategy = LoadBalancerStrategy.SERVICE_LOAD_BALANCER_RANDOM_WEIGHT_ENHANCED;

    @Autowired
    private SmsRecordDao smsRecordDao;
    @Autowired
    private AccountUtils accountUtils;
    @Autowired
    private ServiceLoadBalancerFactory<MessageTypeSmsConfig> serviceLoadBalancer;

    @Autowired
    private AustinMessageSendProperties messageSendProperties;

    public SmsHandler() {
        channelCode = ChannelType.SMS.getCode();
    }

    @Override
    public boolean doHandle(TaskInfo taskInfo) {
        SmsParam smsParam = SmsParam.builder()
                .phones(taskInfo.getReceiver())
                .content(getSmsContent(taskInfo))
                .messageTemplateId(taskInfo.getMessageTemplateId())
                .build();

            /*
              1、动态配置做流量负载
              2、发送短信
             */
        List<MessageTypeSmsConfig> messageTypeSmsConfigs = serviceLoadBalancer.selectService(getMessageTypeSmsConfig(taskInfo), loadBalancerStrategy);
        for (MessageTypeSmsConfig messageTypeSmsConfig : messageTypeSmsConfigs) {
            smsParam.setScriptName(messageTypeSmsConfig.getScriptName());
            smsParam.setSendAccountId(messageTypeSmsConfig.getSendAccount());
            try {
                SmsScript smsScript = applicationContext.getBean(messageTypeSmsConfig.getScriptName(), SmsScript.class);
                List<SmsRecord> recordList = RetryUtils.submitWithRetry(3, 1000, () -> smsScript.send(smsParam));
                if (CollUtil.isNotEmpty(recordList)) {
//                    smsRecordDao.saveAll(recordList);
//                    log.info("调用接口发送短信y")
                    return true;
                }
            } catch (Exception e) {
                log.error("发送短信失败，accountId: {}, phones: {}",
                        messageTypeSmsConfig.getSendAccount(),
                        smsParam.getPhones(), e);
            }

        }

        return false;
    }

    /**
     * 如模板指定具体的明确账号，则优先发其账号，否则走到流量配置
     * <p>
     * 流量配置每种类型都会有其下发渠道账号的配置(流量占比也会配置里面)
     * <p>
     * 样例：
     * key：msgTypeSmsConfig
     * value：[{"message_type_10":[{"weights":80,"scriptName":"TencentSmsScript"},{"weights":20,"scriptName":"YunPianSmsScript"}]},{"message_type_20":[{"weights":20,"scriptName":"YunPianSmsScript"}]},{"message_type_30":[{"weights":20,"scriptName":"TencentSmsScript"}]},{"message_type_40":[{"weights":20,"scriptName":"TencentSmsScript"}]}]
     * 通知类短信有两个发送渠道 TencentSmsScript 占80%流量，YunPianSmsScript占20%流量
     * 营销类短信只有一个发送渠道 YunPianSmsScript
     * 验证码短信只有一个发送渠道 TencentSmsScript
     *
     * @param taskInfo
     * @return
     */
    private List<MessageTypeSmsConfig> getMessageTypeSmsConfig(TaskInfo taskInfo) {

        /*
          如果模板指定了账号，则优先使用具体的账号进行发送
         */
        if (!taskInfo.getSendAccount().equals(AUTO_FLOW_RULE)) {
            SmsAccount account = accountUtils.getAccountById(taskInfo.getSendAccount(), SmsAccount.class);
            return Collections.singletonList(MessageTypeSmsConfig.builder().sendAccount(taskInfo.getSendAccount()).scriptName(account.getScriptName()).weights(100).build());
        }

        var configList = messageSendProperties.getMsgTypeSmsConfig();

        for (var config : configList) {
            var msgType = config.keySet().stream().findFirst().orElse(null);
            if (msgType != null && msgType.getCode().equals(taskInfo.getMsgType())) {
                var res = new ArrayList<MessageTypeSmsConfig>();
                for (var msgTypeSmsConfig : config.get(msgType)) {
                    res.add(
                            MessageTypeSmsConfig.builder()
                                    .scriptName(msgTypeSmsConfig.scriptName())
                                    .weights(msgTypeSmsConfig.weight())
                                    .build()
                    );
                }
                return res;
            }
        }

        return new ArrayList<>();
    }

    /**
     * 如果有输入链接，则把链接拼在文案后
     * <p>
     * PS: 这里可以考虑将链接 转 短链
     * PS: 如果是营销类的短信，需考虑拼接 回TD退订 之类的文案
     */
    private String getSmsContent(TaskInfo taskInfo) {
        SmsContentModel smsContentModel = (SmsContentModel) taskInfo.getContentModel();
        if (CharSequenceUtil.isNotBlank(smsContentModel.getUrl())) {
            return smsContentModel.getContent() + CharSequenceUtil.SPACE + smsContentModel.getUrl();
        } else {
            return smsContentModel.getContent();
        }
    }

    /**
     * 短信不支持撤回
     * 腾讯云文档 eg：<a href="https://cloud.tencent.com/document/product/382/52077">...</a>
     *
     * @param recallTaskInfo
     */
    @Override
    public void recall(RecallTaskInfo recallTaskInfo) {

    }

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
