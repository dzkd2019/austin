package com.java3y.austin.handler.receiver.kafka;

import com.java3y.austin.support.constans.MessageQueuePipeline;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

import java.nio.charset.StandardCharsets;

/**
 * 启动消费者
 *
 * @author 3y
 * &#064;date  2021/12/4
 */
@Configuration
@ConditionalOnProperty(name = "austin.mq.pipeline", havingValue = MessageQueuePipeline.KAFKA)
@EnableKafka
@Slf4j
public class KafkaConfiguration {

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    /**
     * 针对tag消息过滤
     * producer 将tag写进header里
     *
     * @return true 消息将会被丢弃
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> filterContainerFactory(@Value("${austin.business.tagId.key}") String tagIdKey,
                                                                                          @Value("${austin.business.tagId.value}") String tagIdValue,
                                                                                          MdcInterceptor mdcInterceptor) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setAckDiscarded(true);

        factory.setRecordInterceptor(mdcInterceptor);
        factory.setRecordFilterStrategy(consumerRecord -> {
            for (Header header : consumerRecord.headers()) {
                if (header.key().equals(tagIdKey) &&
                        new String(header.value(), StandardCharsets.UTF_8).equals(tagIdValue)) {
                    return false;
                }
            }
            return true;
        });
        return factory;
    }
}
