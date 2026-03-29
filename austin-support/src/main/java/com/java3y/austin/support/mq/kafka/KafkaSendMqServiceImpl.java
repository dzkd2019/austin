package com.java3y.austin.support.mq.kafka;

import cn.hutool.core.text.CharSequenceUtil;
import com.java3y.austin.common.exception.SystemBusyException;
import com.java3y.austin.support.backpressure.RemoteCircuitBreakerManager;
import com.java3y.austin.support.constans.MdcConstant;
import com.java3y.austin.support.constans.MessageQueuePipeline;
import com.java3y.austin.support.mq.MqRateLimiter;
import com.java3y.austin.support.mq.SendMqService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;


/**
 * @author 3y
 * kafka 发送实现类
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "austin.mq.pipeline", havingValue = MessageQueuePipeline.KAFKA)
public class KafkaSendMqServiceImpl implements SendMqService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MqRateLimiter mqRateLimiter;
    private final RemoteCircuitBreakerManager remoteCircuitBreakerManager;

    @Value("${austin.business.tagId.key}")
    private String tagIdKey;

    /**
     * MDC 中存放 traceId 的 key，与 MdcEnrichFilter 保持一致
     */
    private static final String MDC_TRACE_ID = "traceId";


    public KafkaSendMqServiceImpl(KafkaTemplate<String, String> kafkaTemplate, MqRateLimiter mqRateLimiter, RemoteCircuitBreakerManager remoteCircuitBreakerManager) {
        this.kafkaTemplate = kafkaTemplate;
        this.mqRateLimiter = mqRateLimiter;
        this.remoteCircuitBreakerManager = remoteCircuitBreakerManager;
    }

    public void send(String topic, String jsonValue, String tagId) {
        send(topic, jsonValue, tagId, false);
    }

    public void send(String topic, String jsonValue) {
        send(topic, jsonValue, null, false);
    }

    public void send(String topic, String jsonValue, String tagId, boolean blocked) {
        String traceId = MDC.get(MDC_TRACE_ID);

        if (remoteCircuitBreakerManager.isPaused(topic)) {
            log.warn("消费端报告压力过大，暂时停止发送消息");
            throw new SystemBusyException("系统繁忙，请稍后再试");
        }

        boolean acquired = mqRateLimiter.getSemaphore().tryAcquire();
        try {
            if (!acquired) {
                log.warn("全局限流生效，无法获取发送许可，拒绝发送消息");
                throw new SystemBusyException("系统繁忙，请稍后再试");
            }

            if (CharSequenceUtil.isNotBlank(tagId)) {

                List<Header> headers = new ArrayList<>();
                headers.add(new RecordHeader(tagIdKey, tagId.getBytes(StandardCharsets.UTF_8)));
                headers.add(new RecordHeader(MDC_TRACE_ID, traceId.getBytes(StandardCharsets.UTF_8)));

                String xxlJobId = MDC.get(MdcConstant.XXL_JOB_ID);
                if(xxlJobId != null) {
                    headers.add(new RecordHeader(MdcConstant.XXL_JOB_ID, xxlJobId.getBytes(StandardCharsets.UTF_8)));
                }
                
                if (blocked) {
                    kafkaTemplate.send(new ProducerRecord<>(topic, null, null, null, jsonValue, headers))
                            .get();
                } else {
                    kafkaTemplate.send(new ProducerRecord<>(topic, null, null, null, jsonValue, headers));
                }
                return;
            }

            if (blocked) {
                kafkaTemplate.send(new ProducerRecord<>(topic, null, null, null, jsonValue))
                        .get();
            } else {
                kafkaTemplate.send(new ProducerRecord<>(topic, null, null, null, jsonValue));
            }
        } catch (ExecutionException e) {
            throw new RuntimeException("向Kafka主题发送消息失败：" + topic, e);
        } catch (InterruptedException e) {
            throw new RuntimeException("线程在等待Kafka发送结果时被中断了", e);
        } finally {
            if (acquired) {
                mqRateLimiter.getSemaphore().release();
            }
        }
    }
}
