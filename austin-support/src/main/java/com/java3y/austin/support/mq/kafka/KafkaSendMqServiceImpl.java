package com.java3y.austin.support.mq.kafka;

import cn.hutool.core.text.CharSequenceUtil;
import com.java3y.austin.support.constans.MessageQueuePipeline;
import com.java3y.austin.support.mq.MqRateLimiter;
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
public class KafkaSendMqServiceImpl {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MqRateLimiter mqRateLimiter;

//    private static final WeightedRandomUtils<String> random =  new WeightedRandomUtils<>();

    @Value("${austin.business.tagId.key}")
    private String tagIdKey;

    /**
     * MDC 中存放 traceId 的 key，与 MdcEnrichFilter 保持一致
     */
    private static final String MDC_TRACE_ID = "traceId";

//    @PostConstruct
//    public void init() {
//        random.add(96, "success");
//        random.add(2, "fail");
//        random.add(2, "timeout");
//    }

    public KafkaSendMqServiceImpl(KafkaTemplate<String, String> kafkaTemplate, MqRateLimiter mqRateLimiter) {
        this.kafkaTemplate = kafkaTemplate;
        this.mqRateLimiter = mqRateLimiter;
    }

    public boolean send(String topic, String jsonValue, String tagId) {
        return send(topic, jsonValue, tagId, false);
    }

    public boolean send(String topic, String jsonValue) {
        return send(topic, jsonValue, null, false);
    }

    public boolean send(String topic, String jsonValue, String tagId, boolean blocked) {
        String traceId = MDC.get(MDC_TRACE_ID);

//        String status = random.next();


        boolean acquired = mqRateLimiter.getSemaphore().tryAcquire();
        try {
            if (!acquired) {
                log.warn("mq rate-limiter permits exhausted, traceId={}", traceId);
                return false;
            }

//            if("timeout".equals(status)) {
//                throw new NetWorkTimeoutException("发送消息到Kafka超时");
//            }
//            else if("fail".equals(status)) {
//                throw new ExecutionException("模拟发送消息到Kafka失败", new RuntimeException("模拟Kafka发送失败"));
//            }

            if (CharSequenceUtil.isNotBlank(tagId)) {
                List<Header> headers = Collections.singletonList(new RecordHeader(tagIdKey, tagId.getBytes(StandardCharsets.UTF_8)));
                if (blocked) {
                    kafkaTemplate.send(new ProducerRecord<>(topic, null, null, null, jsonValue, headers))
                            .get();
                } else {
                    kafkaTemplate.send(new ProducerRecord<>(topic, null, null, null, jsonValue, headers));
                }
                return true;
            }

            if (blocked) {
                kafkaTemplate.send(new ProducerRecord<>(topic, null, null, null, jsonValue))
                        .get();
            } else {
                kafkaTemplate.send(new ProducerRecord<>(topic, null, null, null, jsonValue));
            }
            return true;
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
