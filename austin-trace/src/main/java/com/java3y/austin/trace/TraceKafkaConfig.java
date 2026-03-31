package com.java3y.austin.trace;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ConditionalOnProperty(name = "austin.business.trace.enabled", value = "true")
public class TraceKafkaConfig {
    @Bean
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public ConcurrentKafkaListenerContainerFactory<String, String> traceContainerFactory(ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setBatchListener(true);

        // 1. 获取全局配置的副本，避免污染其他监听器
        Map<String, Object> props = new HashMap<>(consumerFactory.getConfigurationProperties());
        // 2. 覆盖我们需要的特有属性
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);

        // 3. 构建专属的 ConsumerFactory
        DefaultKafkaConsumerFactory<String, String> traceConsumerFactory = new DefaultKafkaConsumerFactory<>(props);
        factory.setConsumerFactory(traceConsumerFactory);
        factory.getContainerProperties().setPollTimeout(1000);
        return factory;
    }
}
