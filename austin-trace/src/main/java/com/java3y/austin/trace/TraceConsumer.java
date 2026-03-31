package com.java3y.austin.trace;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.util.BinaryData;
import co.elastic.clients.util.ContentType;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(name = "austin.business.trace.enabled", value = "true")
@Slf4j
public class TraceConsumer {
    private ElasticsearchClient elasticsearchClient;

    private BulkIngester<Void> bulkIngester;

    @Value("${austin.business.trace.elastic.host}")
    private String elasticHost;

    @Value("${austin.basiness.trace.elastic.index}")
    private String elasticIndex;
    @Value("${austin.business.trace.elastic.username}")
    private String elasticUsername;
    @Value("${austin.business.trace.elastic.password}")
    private String elasticPassword;

    @PostConstruct
    public void init() {
        this.elasticsearchClient = createClient();

        bulkIngester = BulkIngester.of(f ->
                f.client(elasticsearchClient)
                        .maxOperations(100)
                        .flushInterval(5, TimeUnit.SECONDS)
        );
    }

    private ElasticsearchClient createClient() {
        return ElasticsearchClient.of(f ->
                f.host(elasticHost)
                        .usernameAndPassword(elasticUsername, elasticPassword)
        );
    }

    @KafkaListener(topics = "#{'${austin.business.trace.topic.name}'}", containerFactory = "traceContainerFactory")
    public void consume(List<String> messages) {
        for (String message : messages) {
            bulkIngester.add(b ->
                    b.index(idx ->
                            idx.index(elasticIndex)
                                    .document(BinaryData.of(message.getBytes(), ContentType.APPLICATION_JSON))
                    )
            );
        }
    }

    @PreDestroy
    public void destroy() {
        bulkIngester.close();
        try {
            elasticsearchClient.close();
        } catch (IOException e) {
            log.error("Failed to close Elasticsearch client", e);
        }
    }
}
