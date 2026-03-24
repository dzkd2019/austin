package com.java3y.austin.web.service.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.text.StrPool;
import com.alibaba.fastjson2.JSON;
import com.java3y.austin.common.constant.AustinConstant;
import com.java3y.austin.common.domain.SimpleAnchorInfo;
import com.java3y.austin.common.enums.AnchorState;
import com.java3y.austin.common.enums.ChannelType;
import com.java3y.austin.common.enums.EnumUtil;
import com.java3y.austin.service.api.domain.TraceResponse;
import com.java3y.austin.service.api.service.TraceService;
import com.java3y.austin.support.dao.SmsRecordDao;
import com.java3y.austin.support.domain.MessageTemplate;
import com.java3y.austin.support.domain.SmsRecord;
import com.java3y.austin.support.utils.RedisUtils;
import com.java3y.austin.support.utils.TaskInfoUtils;
import com.java3y.austin.web.service.DataService;
import com.java3y.austin.web.service.MessageTemplateService;
import com.java3y.austin.web.utils.AnchorStateUtils;
import com.java3y.austin.web.utils.Convert4Amis;
import com.java3y.austin.web.vo.DataParam;
import com.java3y.austin.web.vo.amis.EchartsVo;
import com.java3y.austin.web.vo.amis.SmsTimeLineVo;
import com.java3y.austin.web.vo.amis.UserTimeLineVo;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 数据链路追踪获取接口 实现类
 *
 * @author 3y
 */
@Service
public class DataServiceImpl implements DataService {

    private final RedisUtils redisUtils;

    private final SmsRecordDao smsRecordDao;

    private final TraceService traceService;

    private final MessageTemplateService messageTemplateService;

    public DataServiceImpl(RedisUtils redisUtils, SmsRecordDao smsRecordDao, TraceService traceService, MessageTemplateService messageTemplateService) {
        this.redisUtils = redisUtils;
        this.smsRecordDao = smsRecordDao;
        this.traceService = traceService;
        this.messageTemplateService = messageTemplateService;
    }


    @Override
    public UserTimeLineVo getTraceMessageInfo(String messageId) {
        TraceResponse traceResponse = traceService.traceByMessageId(messageId);
        if (CollUtil.isEmpty(traceResponse.getData())) {
            return UserTimeLineVo.builder().items(new ArrayList<>()).build();
        }
        return buildUserTimeLineVo(traceResponse.getData());
    }

    @Override
    public UserTimeLineVo getTraceUserInfo(String receiver) {
        List<String> userInfoList = redisUtils.lRange(receiver, 0, -1);
        if (CollUtil.isEmpty(userInfoList)) {
            return UserTimeLineVo.builder().items(new ArrayList<>()).build();
        }

        // 0. 按时间排序
        List<SimpleAnchorInfo> sortAnchorList = userInfoList.stream().map(s -> JSON.parseObject(s, SimpleAnchorInfo.class)).sorted(Comparator.comparing(SimpleAnchorInfo::getTimestamp).reversed()).collect(Collectors.toList());
        return buildUserTimeLineVo(sortAnchorList);
    }

    @Override
    public EchartsVo getTraceMessageTemplateInfo(String businessId) {

        // 获取businessId并获取模板信息
        businessId = getRealBusinessId(businessId);
        MessageTemplate template = messageTemplateService.queryById(TaskInfoUtils.getMessageTemplateIdFromBusinessId(Long.valueOf(businessId)));
        if (template == null) {
            return null;
        }

        /*
          获取redis清洗好的数据
          key：state
          value:stateCount
         */
        Map<Object, Object> anchorResult = redisUtils.hGetAll(getRealBusinessId(businessId));

        return Convert4Amis.getEchartsVo(anchorResult, template, businessId);
    }

    @Override
    public SmsTimeLineVo getTraceSmsInfo(DataParam dataParam) {

        Integer sendDate = Integer.valueOf(DateUtil.format(new Date(dataParam.getDateTime() * 1000L), DatePattern.PURE_DATE_PATTERN));
        List<SmsRecord> smsRecordList = smsRecordDao.findByPhoneAndSendDate(Long.valueOf(dataParam.getReceiver()), sendDate);
        if (CollUtil.isEmpty(smsRecordList)) {
            return SmsTimeLineVo.builder().items(Collections.singletonList(SmsTimeLineVo.ItemsVO.builder().build())).build();
        }

        Map<String, List<SmsRecord>> maps = smsRecordList.stream().collect(Collectors.groupingBy(o -> o.getPhone() + o.getSeriesId()));
        return Convert4Amis.getSmsTimeLineVo(maps);
    }

    /**
     * 如果传入的是模板ID，则生成【当天】的businessId进行查询
     * 如果传入的是businessId，则按默认的businessId进行查询
     * 判断是否为businessId则判断长度是否为16位（businessId长度固定16)
     */
    private String getRealBusinessId(String businessId) {
        if (AustinConstant.BUSINESS_ID_LENGTH == businessId.length()) {
            return businessId;
        }
        MessageTemplate template = messageTemplateService.queryById(Long.valueOf(businessId));
        if (template != null) {
            return String.valueOf(TaskInfoUtils.generateBusinessId(template.getId(), template.getTemplateType()));
        }
        return businessId;
    }

    private UserTimeLineVo buildUserTimeLineVo(List<SimpleAnchorInfo> sortAnchorList) {
        // 1. 对相同的businessId进行分类  {"businessId":[{businessId,state,timeStamp},{businessId,state,timeStamp}]}
        Map<Long, List<SimpleAnchorInfo>> map1 = sortAnchorList.stream()
                .collect(Collectors.groupingBy(SimpleAnchorInfo::getBusinessId));

        Set<Long> templateIds = map1.keySet().stream()
                .map(TaskInfoUtils::getMessageTemplateIdFromBusinessId)
                .collect(Collectors.toSet());

        Map<Long, MessageTemplate> messageTemplateMap = messageTemplateService.queryByIds(templateIds.toArray(Long[]::new))
                .stream()
                .collect(Collectors.toMap(MessageTemplate::getId, Function.identity()));

        // 2. 封装vo 给到前端渲染展示
        List<UserTimeLineVo.ItemsVO> items = new ArrayList<>();


        for (var entry : map1.entrySet()) {
            MessageTemplate messageTemplate = messageTemplateMap.get(TaskInfoUtils.getMessageTemplateIdFromBusinessId(entry.getKey()));
            StringBuilder sb = new StringBuilder();
            for (SimpleAnchorInfo simpleAnchorInfo : entry.getValue()) {
                if (AnchorState.RECEIVE.getCode().equals(simpleAnchorInfo.getState())) {
                    sb.append(StrPool.CRLF);
                }
                String startTime = DateUtil.format(new Date(simpleAnchorInfo.getTimestamp()), DatePattern.NORM_DATETIME_PATTERN);
                String stateDescription = AnchorStateUtils.getDescriptionByState(messageTemplate.getSendChannel(), simpleAnchorInfo.getState());

                sb.append(startTime).append(StrPool.C_COLON).append(stateDescription).append("==>");
            }

            for (String detail : sb.toString().split(StrPool.CRLF)) {
                if (CharSequenceUtil.isNotBlank(detail)) {
                    UserTimeLineVo.ItemsVO itemsVO = UserTimeLineVo.ItemsVO.builder()
                            .businessId(String.valueOf(entry.getKey()))
                            .sendType(EnumUtil.getEnumByCode(messageTemplate.getSendChannel(), ChannelType.class).getDescription())
                            .creator(messageTemplate.getCreator())
                            .title(messageTemplate.getName())
                            .detail(detail)
                            .build();
                    items.add(itemsVO);
                }
            }
        }
        return UserTimeLineVo.builder().items(items).build();
    }
}
