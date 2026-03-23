package com.java3y.austin.handler.receiver.service.impl;

import com.java3y.austin.common.domain.AnchorInfo;
import com.java3y.austin.common.domain.LogParam;
import com.java3y.austin.common.domain.RecallTaskInfo;
import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.common.enums.AnchorState;
import com.java3y.austin.handler.backpressure.RtSensor;
import com.java3y.austin.handler.backpressure.VirtualThreadBackPressureManager;
import com.java3y.austin.handler.handler.HandlerHolder;
import com.java3y.austin.handler.pending.Task;
import com.java3y.austin.handler.receiver.service.ConsumeService;
import com.java3y.austin.handler.utils.GroupIdMappingUtils;
import com.java3y.austin.support.config.ThreadPoolExecutorShutdownDefinition;
import com.java3y.austin.support.utils.LogUtils;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author 3y
 */
@Service
@Slf4j
public class ConsumeServiceImpl implements ConsumeService {
    private static final String LOG_BIZ_TYPE = "Receiver#consumer";
    private static final String LOG_BIZ_RECALL_TYPE = "Receiver#recall";

    private final ApplicationContext context;

    private final LogUtils logUtils;

    private final HandlerHolder handlerHolder;

    private final VirtualThreadBackPressureManager backPressureManager;

    private final RtSensor rtSensor;

    private final ThreadPoolExecutorShutdownDefinition threadPoolExecutorShutdownDefinition;

    public ConsumeServiceImpl(ApplicationContext context, LogUtils logUtils,
                              HandlerHolder handlerHolder, VirtualThreadBackPressureManager backPressureManager,
                              RtSensor rtSensor, ThreadPoolExecutorShutdownDefinition threadPoolExecutorShutdownDefinition) {
        this.context = context;
        this.logUtils = logUtils;
        this.handlerHolder = handlerHolder;
        this.backPressureManager = backPressureManager;
        this.rtSensor = rtSensor;
        this.threadPoolExecutorShutdownDefinition = threadPoolExecutorShutdownDefinition;
    }

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    @PostConstruct
    public void init() {
        // 注册线程池到关闭钩子中，以便在应用关闭时正确地关闭线程池，避免资源泄漏
        threadPoolExecutorShutdownDefinition.registryExecutor(executor);
    }

    @Override
    public void consume2Send(List<TaskInfo> taskInfoLists) {
        String groupId = GroupIdMappingUtils.getGroupIdByTaskInfo(taskInfoLists.getFirst());
        for (TaskInfo taskInfo : taskInfoLists) {
            backPressureManager.incrementAndCheckPause(groupId);
            logUtils.print(LogParam.builder().bizType(LOG_BIZ_TYPE).object(taskInfo).build(), AnchorInfo.builder().bizId(taskInfo.getBizId()).messageId(taskInfo.getMessageId()).ids(taskInfo.getReceiver()).businessId(taskInfo.getBusinessId()).state(AnchorState.RECEIVE.getCode()).build());
            Task task = context.getBean(Task.class).setTaskInfo(taskInfo);
            long startTime = System.nanoTime();
            executor.execute(() -> {
                try {
                    task.run();
                } finally {
                    long rt = System.nanoTime() - startTime;
                    rtSensor.record(groupId, rt);

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
