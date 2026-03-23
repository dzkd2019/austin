package com.java3y.austin.handler.receiver.service.impl;

import com.java3y.austin.common.domain.AnchorInfo;
import com.java3y.austin.common.domain.LogParam;
import com.java3y.austin.common.domain.RecallTaskInfo;
import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.common.enums.AnchorState;
import com.java3y.austin.handler.backpressure.RtSensor;
import com.java3y.austin.handler.handler.HandlerHolder;
import com.java3y.austin.handler.backpressure.VirtualThreadBackPressureManager;
import com.java3y.austin.handler.pending.Task;
import com.java3y.austin.handler.receiver.service.ConsumeService;
import com.java3y.austin.handler.utils.GroupIdMappingUtils;
import com.java3y.austin.support.utils.LogUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author 3y
 */
@Service
public class ConsumeServiceImpl implements ConsumeService {
    private static final String LOG_BIZ_TYPE = "Receiver#consumer";
    private static final String LOG_BIZ_RECALL_TYPE = "Receiver#recall";

    private final ApplicationContext context;

    private final LogUtils logUtils;

    private final HandlerHolder handlerHolder;

    private final VirtualThreadBackPressureManager backPressureManager;

    private final RtSensor rtSensor;

    public ConsumeServiceImpl(ApplicationContext context, LogUtils logUtils,
                              HandlerHolder handlerHolder, VirtualThreadBackPressureManager backPressureManager, RtSensor rtSensor) {
        this.context = context;
        this.logUtils = logUtils;
        this.handlerHolder = handlerHolder;
        this.backPressureManager = backPressureManager;
        this.rtSensor = rtSensor;
    }

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    @Override
    public void consume2Send(List<TaskInfo> taskInfoLists) {
        String groupId = GroupIdMappingUtils.getGroupIdByTaskInfo(taskInfoLists.getFirst());
        for (TaskInfo taskInfo : taskInfoLists) {
            backPressureManager.incrementAndCheckPause(groupId);
            logUtils.print(LogParam.builder().bizType(LOG_BIZ_TYPE).object(taskInfo).build(), AnchorInfo.builder().bizId(taskInfo.getBizId()).messageId(taskInfo.getMessageId()).ids(taskInfo.getReceiver()).businessId(taskInfo.getBusinessId()).state(AnchorState.RECEIVE.getCode()).build());
            Task task = context.getBean(Task.class).setTaskInfo(taskInfo);
            long startTime = System.currentTimeMillis();
            executor.execute(() -> {
                boolean success = false;
                try {
                    task.run();
                    success = true;
                } finally {
                    if (success) {
                        long rt = System.currentTimeMillis() - startTime;
                        rtSensor.record(rt);
                    }
                    backPressureManager.decrementAndCheckResume(groupId);
                }
            });
        }
    }

    @Override
    public void consume2recall(RecallTaskInfo recallTaskInfo) {
        logUtils.print(LogParam.builder().bizType(LOG_BIZ_RECALL_TYPE).object(recallTaskInfo).build());
        handlerHolder.route(recallTaskInfo.getSendChannel()).recall(recallTaskInfo);
    }
}
