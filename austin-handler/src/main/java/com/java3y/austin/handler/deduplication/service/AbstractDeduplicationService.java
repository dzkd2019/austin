package com.java3y.austin.handler.deduplication.service;

import cn.hutool.core.collection.CollUtil;
import com.java3y.austin.common.domain.AnchorInfo;
import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.common.domain.TraceInfo;
import com.java3y.austin.handler.deduplication.DeduplicationHolder;
import com.java3y.austin.handler.deduplication.DeduplicationParam;
import com.java3y.austin.handler.deduplication.DeduplicationType;
import com.java3y.austin.handler.deduplication.limit.LimitService;
import com.java3y.austin.support.utils.LogUtils;
import com.java3y.austin.support.utils.TraceUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import jakarta.annotation.PostConstruct;
import java.util.Set;

/**
 * @author 3y
 * @date 2021/12/9
 * 去重服务
 */
@Slf4j
public abstract class AbstractDeduplicationService implements DeduplicationService {

    protected DeduplicationType type;

    protected LimitService limitService;

    @Autowired
    private DeduplicationHolder deduplicationHolder;

    @Autowired
    private TraceUtils traceUtils;


    @PostConstruct
    private void init() {
        deduplicationHolder.putService(type, this);
    }

    @Override
    public void deduplication(DeduplicationParam param) {
        TaskInfo taskInfo = param.getTaskInfo();

        Set<String> filterReceiver = limitService.limitFilter(this, taskInfo, param);

        // 剔除符合去重条件的用户
        if (CollUtil.isNotEmpty(filterReceiver)) {
            traceUtils.trace(new TraceInfo(taskInfo, param.getAnchorState(), filterReceiver));
            taskInfo.getReceiver().removeAll(filterReceiver);
            log.info("触发去重规则 {}，下列receivers被过滤：{}", type.name(), String.join(",", taskInfo.getReceiver()));
        }
    }


    /**
     * 构建去重的Key
     *
     */
    public abstract String deduplicationSingleKey(TaskInfo taskInfo, String receiver);


}
