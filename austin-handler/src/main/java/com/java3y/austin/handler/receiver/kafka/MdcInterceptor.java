package com.java3y.austin.handler.receiver.kafka;

import com.java3y.austin.support.constans.MdcConstant;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.jspecify.annotations.Nullable;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class MdcInterceptor implements RecordInterceptor<String, String> {

    @Override
    public @Nullable ConsumerRecord<String, String> intercept(ConsumerRecord<String, String> record, Consumer<String, String> consumer) {
        addToMdc(MdcConstant.MDC_TRACE_ID, record);
        addToMdc(MdcConstant.XXL_JOB_ID, record);

        return record;
    }

    @Override
    public void afterRecord(ConsumerRecord<String, String> record, Consumer<String, String> consumer) {
        MDC.remove(MdcConstant.MDC_TRACE_ID);
        MDC.remove(MdcConstant.XXL_JOB_ID);
    }

    private void addToMdc(String key, ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(key);
        if (header != null) {
            MDC.put(key, new String(header.value(), StandardCharsets.UTF_8));
        } else {
            MDC.remove(key);
        }
    }
}
