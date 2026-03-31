package com.java3y.austin.cron.handler;

import cn.hutool.core.text.CharSequenceUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONException;
import com.alibaba.fastjson2.JSONWriter;
import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.support.constans.MdcConstant;
import com.java3y.austin.support.mq.MqRateLimiter;
import com.java3y.austin.support.mq.SendMqService;
import com.java3y.austin.support.utils.GroupIdMappingUtils;
import com.java3y.austin.support.utils.MdcUtil;
import com.java3y.austin.support.utils.RedisUtils;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * 夜间屏蔽的延迟处理类
 * <p>
 * example:当消息下发至austin平台时，已经是凌晨1点，业务希望此类消息在次日的早上9点推送
 *
 * @author 3y
 */
@Service
@Slf4j
public class NightShieldLazyPendingHandler {

    private static final String NIGHT_SHIELD_BUT_NEXT_DAY_SEND_KEY = "night_shield_send";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Value("austin.business.tagId.value")
    private String tagId;

    @Value("${austin.mq.pending-timeout-ms:" + DEFAULT_PENDING_TIMEOUT_MS + "}")
    private long pendingTimeoutMs;

    @Autowired
    private RedisUtils redisUtils;

    private MqRateLimiter mqRateLimiter;

    private SendMqService sendMqService;

    private static final long DEFAULT_PENDING_TIMEOUT_MS = 3000L;

    /**
     * 处理 夜间屏蔽(次日早上9点发送的任务)
     */
    @XxlJob("nightShieldLazyJob")
    public void execute() {
        log.info("NightShieldLazyPendingHandler#execute 开始处理...");

        // 1. 优化 Redis 拉取：无需 lLen，直接循环 lPop，直到为 null
        // (如果数据量极大，建议使用 lua 脚本或 lRange + lTrim 批量拉取 1000 条，这里保持原意用 lPop)
        List<String> taskInfos = new ArrayList<>();
        String stringInfo;
        while (CharSequenceUtil.isNotBlank(stringInfo = redisUtils.lPop(NIGHT_SHIELD_BUT_NEXT_DAY_SEND_KEY))) {
            taskInfos.add(stringInfo);
        }

        if (taskInfos.isEmpty()) {
            log.info("Redis 队列为空，任务结束。");
            return;
        }

        log.info("共拉取到 {} 条待处理任务，开始并发推送到 Kafka...", taskInfos.size());

        AtomicInteger successCount = new AtomicInteger(0);
        var failed = new ConcurrentLinkedQueue<String>();

        // 2. 真正发挥虚拟线程的威力：并发 I/O！
        // 使用新特性 StructuredTaskScope 保证同生共死，或者直接使用 newVirtualThreadPerTaskExecutor()
        // 这里的 try-with-resources 会【阻塞当前 XXL-JOB 线程】，直到所有虚拟线程执行完毕！
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {

            for (String info : taskInfos) {
                try {
                    TaskInfo taskInfo = JSON.parseObject(info, TaskInfo.class);

                    Map<String, String> mdcContext = new HashMap<>();
                    mdcContext.put(MdcConstant.MDC_MESSAGE_ID, taskInfo.getMessageId());
                    mdcContext.put(MdcConstant.MDC_BUSINESS_ID, String.valueOf(taskInfo.getTraceId()));
                    mdcContext.put(MdcConstant.MDC_KAFKA_GROUP_ID, GroupIdMappingUtils.getGroupIdByTaskInfo(taskInfo));

                    taskInfo.setEnqueueTime(System.currentTimeMillis());
                    // 为【每一条】消息单独提交一个虚拟线程去发送 Kafka
                    executor.execute(MdcUtil.wrap(mdcContext, () -> {
                        try {
                            long waitMs = System.currentTimeMillis() - taskInfo.getEnqueueTime();
                            if (waitMs > pendingTimeoutMs) {
                                log.warn("message timeout before mq send, waitMs={}ms, threshold={}ms",
                                        waitMs, pendingTimeoutMs);
                                return;
                            }

                            String message = JSON.toJSONString(Collections.singletonList(taskInfo), JSONWriter.Feature.WriteClassName);

                            // KafkaTemplate.send 默认是异步的，但它底层获取元数据时会阻塞。
                            // 用 .get() 强制当前虚拟线程阻塞等待发送结果，确保绝对可靠。
                            String topicName = GroupIdMappingUtils.getGroupIdByTaskInfo(taskInfo);
//                            sendMqService.send(topicName, message, tagId, true);
                            sendMqService.send(topicName, message, tagId);
                            successCount.incrementAndGet();
                        } catch (Exception e) {
                            // 这里可以记录失败的数据到另一个错误队列，或者抛出异常让 XXL-JOB 记录失败
                            log.error("nightShieldLazyJob send kafka fail!", e);
                            failed.add(taskInfo.getMessageId());
                        }
                    }));
                } catch (JSONException e) {
                    log.error("TaskInfo JSON 解析失败，跳过该条数据！ info={}", info, e);
                }
            }

        }
        // 离开 try 块时，主线程会自动等待所有 submit 的虚拟线程执行完毕！
        String failedMessageIds = String.join(",", failed);
        log.info("NightShieldLazyPendingHandler#execute 执行完毕！ 总条数：{}, 成功次数: {}, 失败消息：{}", taskInfos.size(), successCount.get(), failedMessageIds);
        // 只有所有消息都发完了（或报错了），这里才会结束，XXL-JOB 控制台才会显示真实的执行耗时。
    }
}
