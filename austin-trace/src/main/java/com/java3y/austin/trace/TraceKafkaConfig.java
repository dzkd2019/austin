package com.java3y.austin.trace;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class TraceKafkaConfig {
    @Bean
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public ConcurrentKafkaListenerContainerFactory<String, String> traceContainerFactory(ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setBatchListener(true);

        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");

        consumerFactory.updateConfigs(configProps);
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setPollTimeout(1000);
        return factory;
    }
}
