package com.java3y.austin.trace;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "austin.business.trace.enabled", havingValue = "true")
public class ElasticSearchConfig {
    @Value("${austin.business.trace.elastic.host}")
    private String elasticHost;

    @Value("${austin.business.trace.elastic.index}")
    private String elasticIndex;
    @Value("${austin.business.trace.elastic.username}")
    private String elasticUsername;
    @Value("${austin.business.trace.elastic.password}")
    private String elasticPassword;

    @Bean
    public ElasticsearchClient elasticsearchClient() {
        return ElasticsearchClient.of(builder ->
                builder.host(elasticHost)
                       .usernameAndPassword(elasticUsername, elasticPassword)
                );
    }
}
