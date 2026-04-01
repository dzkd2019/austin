package com.java3y.austin.web.service.impl.es;

import cn.hutool.core.text.CharSequenceUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.java3y.austin.common.domain.TraceInfo;
import com.java3y.austin.web.service.es.EsTraceService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * ES链路查询实现
 */
@Service
public class EsTraceServiceImpl implements EsTraceService {
    private static final long DEFAULT_RANGE_MILLIS = TimeUnit.HOURS.toMillis(72);

    private final ElasticsearchClient elasticsearchClient;

    @Value("${austin.business.trace.elastic.index}")
    private String index;

    public EsTraceServiceImpl(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }

    @Override
    public List<TraceInfo> traceByMessageId(String messageId, Long startTime, Long endTime) {
        if (CharSequenceUtil.isBlank(messageId)) {
            return new ArrayList<>();
        }
        return searchAllByTerm("messageId", messageId, startTime, endTime);
    }

    @Override
    public List<TraceInfo> traceByTraceId(String traceId, Long startTime, Long endTime) {
        if (CharSequenceUtil.isBlank(traceId)) {
            return new ArrayList<>();
        }
        return searchLatestByTermWithMessageGroup("traceId", traceId, startTime, endTime);
    }

    @Override
    public List<TraceInfo> traceByBizId(String bizId, Long startTime, Long endTime) {
        if (CharSequenceUtil.isBlank(bizId)) {
            return new ArrayList<>();
        }
        return searchLatestByTermWithMessageGroup("bizId", bizId, startTime, endTime);
    }

    @Override
    public List<TraceInfo> traceByTemplateId(Long templateId, Long startTime, Long endTime) {
        if (Objects.isNull(templateId)) {
            return new ArrayList<>();
        }
        return searchLatestByTermWithMessageGroup("templateId", templateId, startTime, endTime);
    }

    @Override
    public List<TraceInfo> traceByReceiver(String receiver, Long startTime, Long endTime) {
        if (CharSequenceUtil.isBlank(receiver)) {
            return new ArrayList<>();
        }
        return searchLatestByTermWithMessageGroup("receivers", receiver, startTime, endTime);
    }

    private List<TraceInfo> searchAllByTerm(String field, Object value, Long startTime, Long endTime) {
        try {
            SearchResponse<TraceInfo> response = elasticsearchClient.search(f ->
                            f
                                    .index(index)
                                    .query(buildBoolQuery(field, value, startTime, endTime))
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

    /**
     * 查询条件命中后，先按 timeStamp 倒序，再按 messageId 折叠，得到每个 messageId 最新的一条。
     *
     * @param field 查询字段
     * @param value 查询值
     * @param startTime 查询起始时间（毫秒）
     * @param endTime 查询结束时间（毫秒）
     */
    private List<TraceInfo> searchLatestByTermWithMessageGroup(String field, Object value, Long startTime, Long endTime) {
        try {
            SearchResponse<TraceInfo> response = elasticsearchClient.search(f ->
                            f
                                    .index(index)
                                    .query(buildBoolQuery(field, value, startTime, endTime))
                                    .sort(s ->
                                            s.field(fi -> fi.field("timeStamp").order(SortOrder.Desc))
                                    )
                                    .collapse(c -> c.field("messageId"))
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

    private Query buildBoolQuery(String field, Object value, Long startTime, Long endTime) {
        long now = System.currentTimeMillis();
        long from;
        long to;
        if (Objects.nonNull(startTime) && Objects.nonNull(endTime)) {
            from = startTime;
            to = endTime;
        } else if (Objects.nonNull(startTime)) {
            from = startTime;
            to = startTime + DEFAULT_RANGE_MILLIS;
        } else if (Objects.nonNull(endTime)) {
            to = endTime;
            from = endTime - DEFAULT_RANGE_MILLIS;
        } else {
            to = now;
            from = now - DEFAULT_RANGE_MILLIS;
        }
        if (from > to) {
            return Query.of(q -> q.matchNone(m -> m));
        }
        return Query.of(q -> q.bool(b -> b
                .must(buildTermQuery(field, value))
                .must(buildTimeRangeQuery(from, to))
        ));
    }

    private Query buildTimeRangeQuery(long startTime, long endTime) {
        // Elasticsearch Java Client NumberRangeQuery 需要 double 边界值，这里将毫秒时间戳(long)显式转换为 double。
        return Query.of(q -> q.range(RangeQuery.of(r -> r
                .number(n -> n
                        .field("timeStamp")
                        .gte((double) startTime)
                        .lte((double) endTime)
                ))));
    }
}
