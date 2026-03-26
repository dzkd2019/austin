package com.java3y.austin.handler.backpressure;

import cn.hutool.core.thread.ThreadFactoryBuilder;
import com.java3y.austin.handler.utils.GroupIdMappingUtils;
import com.java3y.austin.support.config.ThreadPoolExecutorShutdownDefinition;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class AimdCruiseController {

    private final VirtualThreadBackPressureManager backPressureManager;
    private final RtSensor rtSensor;

    // 调度线程池 (单线程即可)
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                    .setNamePrefix("AIMD-Cruise-Controller-%d")
                    .build()
    );

    // === AIMD 算法阈值与步长配置 ===

    // RT 阈值 (毫秒)
    private static final long RT_HEALTHY_MAX = 100L;  // 健康水位，小于此值尝试提速
    private static final long RT_DANGER_MIN = 300L;   // 危险水位，大于此值立刻降速

    // 水位线绝对物理边界 (保护 JVM 本身)
    private static final int ABSOLUTE_MIN_HIGH_WATERMARK = 100;
    private static final int ABSOLUTE_MAX_HIGH_WATERMARK = 200_000;

    // 调节步长
    private static final int ADDITIVE_INCREASE_STEP = 2000; // 每次提速增加的并发数
    private static final double MULTIPLICATIVE_DECREASE_FACTOR = 0.5; // 每次降速砍掉的比例

    private static final double LOW_WATERMARK_PERCENTAGE = 0.7;

    private final ThreadPoolExecutorShutdownDefinition shutdownDefinition;

    public AimdCruiseController(VirtualThreadBackPressureManager manager, RtSensor sensor, ThreadPoolExecutorShutdownDefinition shutdownDefinition) {
        this.backPressureManager = manager;
        this.rtSensor = sensor;
        this.shutdownDefinition = shutdownDefinition;
    }

    @PostConstruct
    public void startCruising() {
        shutdownDefinition.registryExecutor(scheduler);
        // 每 5 秒巡航一次
        scheduler.scheduleAtFixedRate(this::checkAndAdjust, 5, 5, TimeUnit.SECONDS);
        log.info("AIMD 巡航控制器已启动，周期: 5秒");
    }

    private void checkAndAdjust() {
        List<String> groupIds = GroupIdMappingUtils.getAllGroupIds();
        for (String groupId : groupIds) {
            try {
                long avgRt = rtSensor.getAndResetAvgRtMs(groupId);

                // 如果期间没有请求，保持原样
                if (avgRt == 0) continue;

                int currentHigh = backPressureManager.getWaterMarkConfig(groupId).highWaterMark();
                int newHigh = currentHigh;

                if (avgRt > RT_DANGER_MIN) {
                    // 【乘性减】：响应太慢了，下游快崩溃了，立刻砍半！
                    newHigh = (int) (currentHigh * MULTIPLICATIVE_DECREASE_FACTOR);
                    log.warn("巡航警报: 当前group {}, 平均 RT ({}ms) 超过危险阈值 ({}ms)！断崖式降载: {} -> {}",
                            groupId, avgRt, RT_DANGER_MIN, currentHigh, newHigh);
                } else if (avgRt < RT_HEALTHY_MAX) {
                    // 【加性增】：响应很快，下游很闲，慢慢增加并发度
                    newHigh = currentHigh + ADDITIVE_INCREASE_STEP;
                    log.info("巡航提速: 当前group {}, 当前平均 RT ({}ms) 表现优异。尝试提速: {} -> {}",
                            groupId, avgRt, currentHigh, newHigh);
                } else {
                    // RT 在 100 ~ 300 之间，属于平稳期，不增不减
                    log.debug("巡航平稳: 当前group {}, 当前平均 RT ({}ms)，水位保持在 {}", groupId, avgRt, currentHigh);
                }

                // 限制绝对边界，防止计算溢出或跌破下限
                newHigh = Math.clamp(newHigh, ABSOLUTE_MIN_HIGH_WATERMARK, ABSOLUTE_MAX_HIGH_WATERMARK);

                // 如果高水位发生了变化，计算新的低水位并更新
                if (newHigh != currentHigh) {
                    // 低水位固定设置为高水位的 70%，保证有一定的呼吸空间防抖
                    int newLow = (int) (newHigh * LOW_WATERMARK_PERCENTAGE);
                    backPressureManager.setWaterMark(groupId, newHigh, newLow);
                }
            } catch (Exception e) {
                log.error("AIMD 巡航控制处理 group [{}] 时发生异常", groupId, e);
            }
        }
    }
}