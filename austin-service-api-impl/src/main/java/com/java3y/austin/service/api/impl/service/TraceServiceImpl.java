package com.java3y.austin.service.api.impl.service;

import cn.hutool.core.text.CharSequenceUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.java3y.austin.common.domain.TraceInfo;
import com.java3y.austin.common.enums.RespStatusEnum;
import com.java3y.austin.service.api.domain.TraceResponse;
import com.java3y.austin.service.api.service.TraceService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: sky
 * @Date: 2023/7/13 13:45
 * @Description: TraceServiceImpl
 * @Version 1.0.0
 */
@Service
@Primary
public class TraceServiceImpl implements TraceService {

    private final ElasticsearchClient elasticsearchClient;

    @Value("${austin.business.trace.elastic.index}")
    private String index;

    public TraceServiceImpl(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }

    @Override
    public TraceResponse traceByMessageId(String messageId) {
//        if (CharSequenceUtil.isBlank(messageId)) {
//            return new TraceResponse(RespStatusEnum.CLIENT_BAD_PARAMETERS.getCode(), RespStatusEnum.CLIENT_BAD_PARAMETERS.getMsg(), null);
//        }
//        String redisMessageKey = CharSequenceUtil.join(StrUtil.COLON, AustinConstant.CACHE_KEY_PREFIX, AustinConstant.MESSAGE_ID, messageId);
//        List<String> messageList = redisUtils.lRange(redisMessageKey, 0, -1);
//        if (CollUtil.isEmpty(messageList)) {
//            return new TraceResponse(RespStatusEnum.FAIL.getCode(), RespStatusEnum.FAIL.getMsg(), null);
//        }
//
//        // 0. 按时间排序
//        List<SimpleAnchorInfo> sortAnchorList = messageList.stream().map(s -> JSON.parseObject(s, SimpleAnchorInfo.class)).sorted((o1, o2) -> Math.toIntExact(o1.getTimestamp() - o2.getTimestamp())).collect(Collectors.toList());

        return new TraceResponse(RespStatusEnum.SUCCESS.getCode(), RespStatusEnum.SUCCESS.getMsg(), List.of());
    }

    public List<TraceInfo> traceByTraceId(String traceId) {
        if (CharSequenceUtil.isBlank(traceId)) {
            return new ArrayList<>();
        }
        try {
            SearchResponse<TraceInfo> response = elasticsearchClient.search(f ->
                            f
                                    .index(index)
                                    .query(q ->
                                            q.term(t ->
                                                    t.field("traceId")
                                                            .value(traceId)
                                            )
                                    )
                                    .sort(s ->
                                            s.field(fi -> fi.field("timeStamp").order(SortOrder.Desc))
                                    )
                                    .collapse(c -> c.field("messageId"))
                    ,
                    TraceInfo.class
            );

            return response.hits().hits().stream()
                    .map(Hit::source)
                    .toList();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
