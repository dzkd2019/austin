package com.java3y.austin.handler.receiver.service.impl;

import com.java3y.austin.common.domain.AnchorInfo;
import com.java3y.austin.common.domain.LogParam;
import com.java3y.austin.common.domain.RecallTaskInfo;
import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.common.enums.AnchorState;
import com.java3y.austin.handler.backpressure.RtSensor;
import com.java3y.austin.handler.backpressure.VirtualThreadBackPressureManager;
import com.java3y.austin.handler.handler.HandlerHolder;
import com.java3y.austin.handler.handler.Task;
import com.java3y.austin.handler.receiver.service.ConsumeService;
import com.java3y.austin.handler.utils.GroupIdMappingUtils;
import com.java3y.austin.handler.utils.MdcUtil;
import com.java3y.austin.support.config.ThreadPoolExecutorShutdownDefinition;
import com.java3y.austin.support.constans.MdcConstant;
import com.java3y.austin.support.utils.LogUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    private static final String TAG_GROUP_ID = "group.id";
    private static final String TAG_RESULT = "result";

    private final ApplicationContext context;
    private final LogUtils logUtils;
    private final HandlerHolder handlerHolder;
    private final VirtualThreadBackPressureManager backPressureManager;
    private final RtSensor rtSensor;
    private final ThreadPoolExecutorShutdownDefinition threadPoolExecutorShutdownDefinition;
    private final MeterRegistry meterRegistry;

    /** groupId -> success counter */
    private final Map<String, Counter> successCounters = new ConcurrentHashMap<>();
    /** groupId -> failure counter */
    private final Map<String, Counter> failureCounters = new ConcurrentHashMap<>();
    /** groupId -> message processing timer */
    private final Map<String, Timer> processTimers = new ConcurrentHashMap<>();

    public ConsumeServiceImpl(ApplicationContext context, LogUtils logUtils,
                              HandlerHolder handlerHolder, VirtualThreadBackPressureManager backPressureManager,
                              RtSensor rtSensor, ThreadPoolExecutorShutdownDefinition threadPoolExecutorShutdownDefinition,
                              MeterRegistry meterRegistry) {
        this.context = context;
        this.logUtils = logUtils;
        this.handlerHolder = handlerHolder;
        this.backPressureManager = backPressureManager;
        this.rtSensor = rtSensor;
        this.threadPoolExecutorShutdownDefinition = threadPoolExecutorShutdownDefinition;
        this.meterRegistry = meterRegistry;
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
        Map<String, String> mdcContext = new HashMap<>();
        for (TaskInfo taskInfo : taskInfoLists) {
            backPressureManager.incrementAndCheckPause(groupId);
            // 打点记录当前状态
            logUtils.print(LogParam.builder().bizType(LOG_BIZ_TYPE).object(taskInfo).build(), AnchorInfo.builder().bizId(taskInfo.getBizId()).messageId(taskInfo.getMessageId()).ids(taskInfo.getReceiver()).businessId(taskInfo.getBusinessId()).state(AnchorState.RECEIVE.getCode()).build());
            Task task = context.getBean(Task.class).setTaskInfo(taskInfo);

            mdcContext.clear();
            mdcContext.put(MdcConstant.MDC_MESSAGE_ID, taskInfo.getMessageId());
            mdcContext.put(MdcConstant.MDC_BUSINESS_ID, String.valueOf(taskInfo.getBusinessId()));
            mdcContext.put(MdcConstant.MDC_KAFKA_GROUP_ID, groupId);
            executor.execute(MdcUtil.wrap(new HashMap<>(mdcContext), () -> {
                // startTime 必须在虚拟线程内部捕获，否则会将排队等待时间计入 RT，
                // 导致 AIMD 控制器误判系统过载而触发不必要的降速。
                long startTime = System.nanoTime();
                try {
                    task.run();
                    getSuccessCounter(groupId).increment();
                } catch (Exception e) {
                    getFailureCounter(groupId).increment();
                    log.error("consume2Send: task execute failed, groupId={}, messageId={}, bizId={}",
                            groupId, taskInfo.getMessageId(), taskInfo.getBizId(), e);
                } finally {
                    // 记录处理时间，供巡航器使用
                    long rtNano = System.nanoTime() - startTime;
                    long rt = rtNano / 1_000_000;
                    rtSensor.record(groupId, rt);
                    getProcessTimer(groupId).record(Duration.ofNanos(rtNano));

                    backPressureManager.decrementAndCheckResume(groupId);
                }
            }));
        }
    }

    @Override
    public void consume2recall(RecallTaskInfo recallTaskInfo) {
        logUtils.print(LogParam.builder().bizType(LOG_BIZ_RECALL_TYPE).object(recallTaskInfo).build());
        handlerHolder.route(recallTaskInfo.getSendChannel()).recall(recallTaskInfo);
    }

    private Counter getSuccessCounter(String groupId) {
        return successCounters.computeIfAbsent(groupId, gid ->
                Counter.builder("austin.kafka.message.consumed")
                        .description("Kafka 消息成功消费数")
                        .tag(TAG_GROUP_ID, gid)
                        .tag(TAG_RESULT, "success")
                        .register(meterRegistry));
    }

    private Counter getFailureCounter(String groupId) {
        return failureCounters.computeIfAbsent(groupId, gid ->
                Counter.builder("austin.kafka.message.consumed")
                        .description("Kafka 消息消费失败数")
                        .tag(TAG_GROUP_ID, gid)
                        .tag(TAG_RESULT, "failure")
                        .register(meterRegistry));
    }

    private Timer getProcessTimer(String groupId) {
        return processTimers.computeIfAbsent(groupId, gid ->
                Timer.builder("austin.kafka.message.process.duration")
                        .description("Kafka 消息处理耗时（虚拟线程内）")
                        .tag(TAG_GROUP_ID, gid)
                        .publishPercentiles(0.5, 0.95, 0.99)
                        .publishPercentileHistogram()
                        .minimumExpectedValue(Duration.ofMillis(1))
                        .maximumExpectedValue(Duration.ofSeconds(30))
                        .register(meterRegistry));
    }
}
