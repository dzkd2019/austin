package com.java3y.austin.trace;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.util.BinaryData;
import co.elastic.clients.util.ContentType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Component
@ConditionalOnProperty(name = "austin.business.trace.enabled", havingValue = "true")
@Slf4j
public class TraceConsumer {
    private final ElasticsearchClient elasticsearchClient;

    @Value("${austin.business.trace.elastic.index}")
    private String elasticIndex;

    public TraceConsumer(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }

    @KafkaListener(topics = "#{'${austin.business.trace.topic.name}'}", containerFactory = "traceContainerFactory", groupId = "#{'${austin.business.trace.group.name}'}")
    public void consume(List<String> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        BulkRequest.Builder builder = new BulkRequest.Builder();
        for (String message : messages) {
            builder.operations(op ->
                    op.index(idx ->
                            idx.index(elasticIndex)
                                    .document(BinaryData.of(message.getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON))
                            )
                    );
        }

        try {
            BulkResponse bulkResponse = elasticsearchClient.bulk(builder.build());
            if (bulkResponse.errors()) {
                bulkResponse
                        .items()
                        .stream()
                        .filter(it -> it.error() != null)
                        .forEach(it -> log.error("ES 写入失败: index={}, id={}, error={}", it.index(), it.id(), it.error().reason()));
            }
        } catch (Exception e) {
            log.error("批量写入 ES 发生系统级异常", e);
            throw new RuntimeException("ES 写入失败，触发 Kafka 消费重试", e); // 抛出异常，阻止 Kafka 提交 Offset
        }
    }
}
