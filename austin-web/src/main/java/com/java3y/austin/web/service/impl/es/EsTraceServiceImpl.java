package com.java3y.austin.web.service.impl.es;

import cn.hutool.core.text.CharSequenceUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.java3y.austin.common.domain.TraceInfo;
import com.java3y.austin.web.service.es.EsTraceService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * ES链路查询实现
 */
@Service
public class EsTraceServiceImpl implements EsTraceService {

    private final ElasticsearchClient elasticsearchClient;

    @Value("${austin.business.trace.elastic.index}")
    private String index;

    public EsTraceServiceImpl(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }

    @Override
    public List<TraceInfo> traceByMessageId(String messageId) {
        if (CharSequenceUtil.isBlank(messageId)) {
            return new ArrayList<>();
        }
        return searchAllByTerm("messageId", messageId);
    }

    @Override
    public List<TraceInfo> traceByTraceId(String traceId) {
        if (CharSequenceUtil.isBlank(traceId)) {
            return new ArrayList<>();
        }
        return searchLatestOneByTerm("traceId", traceId);
    }

    @Override
    public List<TraceInfo> traceByBizId(String bizId) {
        if (CharSequenceUtil.isBlank(bizId)) {
            return new ArrayList<>();
        }
        return searchLatestOneByTerm("bizId", bizId);
    }

    @Override
    public List<TraceInfo> traceByTemplateId(Long templateId) {
        if (Objects.isNull(templateId)) {
            return new ArrayList<>();
        }
        return searchLatestOneByTerm("templateId", templateId);
    }

    @Override
    public List<TraceInfo> traceByReceiver(String receiver) {
        if (CharSequenceUtil.isBlank(receiver)) {
            return new ArrayList<>();
        }
        return searchLatestOneByTerm("receivers", receiver);
    }

    private List<TraceInfo> searchAllByTerm(String field, Object value) {
        try {
            SearchResponse<TraceInfo> response = elasticsearchClient.search(f ->
                            f
                                    .index(index)
                                    .query(buildTermQuery(field, value))
                    ,
                    TraceInfo.class
            );

            return response.hits().hits().stream()
                    .map(Hit::source)
                    .filter(Objects::nonNull)
                    .toList();
        } catch (IOException e) {
            throw new RuntimeException("Failed to search Elasticsearch traces by field: " + field, e);
        }
    }

    private List<TraceInfo> searchLatestOneByTerm(String field, Object value) {
        try {
            SearchResponse<TraceInfo> response = elasticsearchClient.search(f ->
                            f
                                    .index(index)
                                    .query(buildTermQuery(field, value))
                                    .sort(s ->
                                            s.field(fi -> fi.field("timeStamp").order(SortOrder.Desc))
                                    )
                                    .size(1)
                    ,
                    TraceInfo.class
            );

            return response.hits().hits().stream()
                    .map(Hit::source)
                    .filter(Objects::nonNull)
                    .toList();
        } catch (IOException e) {
            throw new RuntimeException("Failed to search latest Elasticsearch trace by field: " + field, e);
        }
    }

    private Query buildTermQuery(String field, Object value) {
        return Query.of(q -> q.term(t -> {
            t.field(field);
            if (value instanceof Long longValue) {
                t.value(v -> v.longValue(longValue));
            } else if (value instanceof Integer intValue) {
                t.value(v -> v.longValue(intValue.longValue()));
            } else {
                t.value(v -> v.stringValue(String.valueOf(value)));
            }
            return t;
        }));
    }
}
