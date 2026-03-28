package com.java3y.austin.cron.handler;

import cn.hutool.core.text.CharSequenceUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.support.mq.MqRateLimiter;
import com.java3y.austin.support.mq.SendMqService;
import com.java3y.austin.support.utils.RedisUtils;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;


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
    private SendMqService sendMqService;
    @Autowired
    private MqRateLimiter mqRateLimiter;
    @Value("${austin.business.topic.name}")
    private String topicName;
    @Value("${austin.business.tagId.value}")
    private String tagId;
    @Autowired
    private RedisUtils redisUtils;

    /**
     * 处理 夜间屏蔽(次日早上9点发送的任务)
     */
    @XxlJob("nightShieldLazyJob")
    public void execute() {
        log.info("NightShieldLazyPendingHandler#execute 开始处理...");

        List<String> taskInfos = new ArrayList<>();
        String taskInfo;
        while (CharSequenceUtil.isNotBlank(taskInfo = redisUtils.lPop(NIGHT_SHIELD_BUT_NEXT_DAY_SEND_KEY))) {
            taskInfos.add(taskInfo);
        }

        if (taskInfos.isEmpty()) {
            log.info("Redis 队列为空，任务结束。");
            return;
        }

        log.info("共拉取到 {} 条待处理任务，开始并发推送到 MQ...", taskInfos.size());

        // try-with-resources 会阻塞当前 XXL-JOB 线程，直到所有虚拟线程执行完毕
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (String info : taskInfos) {
                executor.submit(() -> sendWithRateLimit(info));
            }
        }

        log.info("NightShieldLazyPendingHandler#execute 执行完毕！");
    }

    /**
     * 通过全局 {@link MqRateLimiter} 信号量保护后再发送，与 API 调用路径保持统一限流策略。
     */
    private void sendWithRateLimit(String info) {
        boolean acquired = mqRateLimiter.getSemaphore().tryAcquire();
        try {
            if (!acquired) {
                log.warn("nightShieldLazyJob: mq rate-limiter permits exhausted, skipping message. params:{}", info);
                return;
            }
            TaskInfo parsedTask = JSON.parseObject(info, TaskInfo.class);
            String message = JSON.toJSONString(Collections.singletonList(parsedTask), JSONWriter.Feature.WriteClassName);
            sendMqService.send(topicName, message, tagId);
        } catch (Exception e) {
            log.error("nightShieldLazyJob send mq fail! params:{}", info, e);
            throw new IllegalStateException("nightShieldLazyJob send mq fail", e);
        } finally {
            if (acquired) {
                mqRateLimiter.getSemaphore().release();
            }
        }
    }
}
