package com.java3y.austin.support.mq.kafka;

import cn.hutool.core.text.CharSequenceUtil;
import com.java3y.austin.common.exception.CommonException;
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


    public KafkaSendMqServiceImpl(KafkaTemplate<String, String> kafkaTemplate, MqRateLimiter mqRateLimiter, RemoteCircuitBreakerManager remoteCircuitBreakerManager) {
        this.kafkaTemplate = kafkaTemplate;
        this.mqRateLimiter = mqRateLimiter;
        this.remoteCircuitBreakerManager = remoteCircuitBreakerManager;
    }

    @Override
    public void send(String topic, String jsonValue, String tagId) {
        send(topic, jsonValue, tagId, true);
    }

    @Override
    public void send(String topic, String jsonValue) {
        send(topic, jsonValue, null, true);
    }

    @Override
    public void asyncSend(String topic, String jsonValue, String tagId) {
        send(topic, jsonValue, tagId, false);
    }

    @Override
    public void asyncSend(String topic, String jsonValue) {
        send(topic, jsonValue, null, false);
    }

    private void send(String topic, String jsonValue, String tagId, boolean blocked) {
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

            List<Header> headers = new ArrayList<>();
            String traceId = MDC.get(MdcConstant.MDC_TRACE_ID);
            String xxlJobId = MDC.get(MdcConstant.XXL_JOB_ID);

            if (CharSequenceUtil.isNotEmpty(tagId)) {
                headers.add(new RecordHeader(tagIdKey, tagId.getBytes(StandardCharsets.UTF_8)));
            }
            if (CharSequenceUtil.isNotBlank(traceId)) {
                headers.add(new RecordHeader(MdcConstant.MDC_TRACE_ID, traceId.getBytes(StandardCharsets.UTF_8)));
            }

            if (CharSequenceUtil.isNotBlank(xxlJobId)) {
                headers.add(new RecordHeader(MdcConstant.XXL_JOB_ID, xxlJobId.getBytes(StandardCharsets.UTF_8)));
            }

            var record = new ProducerRecord<String, String>(topic, null, null, null, jsonValue, headers);


            if (blocked) {
                kafkaTemplate.send(record).get();
            } else {
                kafkaTemplate.send(record)
                        .whenComplete((_, ex) -> {
                            if (ex != null) {
                                log.error("向Kafka主题发送消息失败，topic={}, traceId={}", topic, traceId, ex);
                            }
                        });
            }
        } catch (ExecutionException e) {
            throw new CommonException("向Kafka主题发送消息失败：" + topic, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CommonException("线程在等待Kafka发送结果时被中断了", e);
        } finally {
            if (acquired) {
                mqRateLimiter.getSemaphore().release();
            }
        }
    }
}
