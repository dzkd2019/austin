package com.java3y.austin.handler.receiver.kafka;

import com.java3y.austin.support.constans.MessageQueuePipeline;
import com.java3y.austin.support.utils.GroupIdMappingUtils;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.beans.BeansException;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@ConditionalOnProperty(name = "austin.mq.pipeline", havingValue = MessageQueuePipeline.KAFKA)
public class KafkaBusinessListenerRegistrar implements ApplicationContextAware, ApplicationRunner {
    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }


    @Override
    public void run(ApplicationArguments args) throws Exception {
        List<String> groupIds = GroupIdMappingUtils.getAllGroupIds();

        for (String groupId : groupIds) {
            try {
                String id = "austin-kafka-" + groupId;
                applicationContext.getBean(Receiver.class, id, groupId, groupId);
            } catch (Exception e) {
                log.error("Kafka 消费者创建失败，topic: {}", groupId, e);
            }
        }
        log.info("所有 Kafka 消费者动态注册完成");
    }
}
