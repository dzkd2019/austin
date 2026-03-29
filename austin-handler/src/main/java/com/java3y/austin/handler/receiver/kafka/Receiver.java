package com.java3y.austin.handler.receiver.kafka;

import cn.hutool.core.collection.CollUtil;
import com.alibaba.fastjson2.JSON;
import com.java3y.austin.common.constant.KafkaTopicConstants;
import com.java3y.austin.common.domain.RecallTaskInfo;
import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.handler.receiver.MessageReceiver;
import com.java3y.austin.handler.receiver.service.ConsumeService;
import com.java3y.austin.support.constans.MessageQueuePipeline;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

/**
 * @author 3y
 * 消费MQ的消息
 */
@Slf4j
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@ConditionalOnProperty(name = "austin.mq.pipeline", havingValue = MessageQueuePipeline.KAFKA)
public class Receiver implements MessageReceiver {
    @Autowired
    private ConsumeService consumeService;

    /**
     * 发送消息
     * todo 解析mdc
     */
    @KafkaListener(topics = KafkaTopicConstants.IM_NOTICE, containerFactory = "filterContainerFactory")
    public void consumer(ConsumerRecord<?, String> consumerRecord) {
        Optional<String> kafkaMessage = Optional.ofNullable(consumerRecord.value());
        if (kafkaMessage.isEmpty()) {
            return;
        }
        try {
            List<TaskInfo> taskInfoLists = JSON.parseArray(kafkaMessage.get(), TaskInfo.class);
            // taskInfoLists 为 null（JSON 解析失败）或空时跳过，避免后续 NPE / NoSuchElementException
            if (CollUtil.isEmpty(taskInfoLists)) {
                log.warn("consumer: received empty or unparseable message, offset={}", consumerRecord.offset());
                return;
            }
            consumeService.consume2Send(taskInfoLists);
        } catch (Exception e) {
            log.error("consumer: failed to process message, offset={}, payload={}",
                    consumerRecord.offset(), kafkaMessage.get(), e);
        }
    }

    /**
     * 撤回消息
     *
     * @param consumerRecord
     */
    @KafkaListener(topics = "#{'${austin.business.recall.topic.name}'}", groupId = "#{'${austin.business.recall.group.name}'}", containerFactory = "filterContainerFactory")
    public void recall(ConsumerRecord<?, String> consumerRecord) {
        Optional<String> kafkaMessage = Optional.ofNullable(consumerRecord.value());
        if (!kafkaMessage.isPresent()) {
            return;
        }
        try {
            RecallTaskInfo recallTaskInfo = JSON.parseObject(kafkaMessage.get(), RecallTaskInfo.class);
            if (recallTaskInfo == null) {
                log.warn("recall: received unparseable message, offset={}", consumerRecord.offset());
                return;
            }
            consumeService.consume2recall(recallTaskInfo);
        } catch (Exception e) {
            log.error("recall: failed to process message, offset={}, payload={}",
                    consumerRecord.offset(), kafkaMessage.get(), e);
        }
    }
}
