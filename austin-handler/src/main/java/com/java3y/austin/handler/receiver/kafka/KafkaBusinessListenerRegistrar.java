package com.java3y.austin.handler.receiver.kafka;

import com.java3y.austin.support.constans.MessageQueuePipeline;
import com.java3y.austin.support.utils.GroupIdMappingUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.SimpleKafkaListenerEndpoint;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@ConditionalOnProperty(name = "austin.mq.pipeline", havingValue = MessageQueuePipeline.KAFKA)
public class KafkaBusinessListenerRegistrar implements ApplicationRunner {

    private static final String BUSINESS_LISTENER_ID_PREFIX = "kafka-business-";

    @Autowired
    private Receiver receiver;
    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, String> filterContainerFactory;

    @Override
    @SuppressWarnings("unchecked")
    public void run(ApplicationArguments args) {
        for (String topic : GroupIdMappingUtils.getAllGroupIds()) {
            String listenerId = BUSINESS_LISTENER_ID_PREFIX + topic;
            if (kafkaListenerEndpointRegistry.getListenerContainer(listenerId) != null) {
                continue;
            }

            SimpleKafkaListenerEndpoint<String, String> endpoint = new SimpleKafkaListenerEndpoint<>();
            endpoint.setId(listenerId);
            endpoint.setGroupId(topic);
            endpoint.setTopics(topic);
            endpoint.setBean(receiver);
            endpoint.setMessageListener((MessageListener<String, String>) receiver::consumer);

            kafkaListenerEndpointRegistry.registerListenerContainer(endpoint, filterContainerFactory, true);
            log.info("registered kafka business listener: id={}, groupId={}, topic={}", listenerId, topic, topic);
        }
    }
}
