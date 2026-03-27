package com.java3y.austin.handler.flowcontrol;

import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.handler.enums.RateLimitStrategy;
import com.java3y.austin.handler.flowcontrol.config.RateLimiterConfig;
import com.java3y.austin.handler.flowcontrol.controller.FlowController;
import com.java3y.austin.handler.flowcontrol.controller.TokenBucketFlowController;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 3y
 * @date 2022/4/18
 * <p>
 */
@Service
@Slf4j
public class FlowControlFactory {

    // 存储维度：渠道(ChannelId) -> 具体的限流器实例
    private final Map<Integer, FlowController> channelLimiterMap = new ConcurrentHashMap<>();

    /**
     * 核心路由与执行方法
     * * @param channelId 渠道唯一标识 (如: "tencent_sms_01")
     *
     * @param param Handler 声明的限流策略, 当前的限流配置 (可能是 Handler 的默认配置，也可能是来自 Nacos 的最新配置)
     */
    public void flowControl(TaskInfo taskInfo, FlowControlParam param) throws InterruptedException {
        RateLimitStrategy strategy = param.getRateLimitStrategy();
        RateLimiterConfig config = param.getRateLimiterConfig();
        Integer channelId = taskInfo.getSendChannel();

        // 1. 如果策略是不限流，直接放行
        if (strategy == RateLimitStrategy.NONE || config == null) {
            return;
        }

        // 2. 获取或创建该渠道专属的限流器
        FlowController controller = channelLimiterMap.computeIfAbsent(channelId, _ -> createController(strategy, config));

        if (controller == null) {
            return;
        }

        // 3. 极其轻量的配置更新检查（内部有 equals 判断，日常调用零开销）
        controller.updateConfig(config);

        // 4. 执行限流阻塞（虚拟线程挂起）

        double cost = controller.acquire();
        log.info("渠道 [{}] 流量控制耗时: {} 毫秒", channelId, cost);

    }

    /**
     * 策略路由工厂方法
     */
    private FlowController createController(RateLimitStrategy strategy, RateLimiterConfig config) {
        // 使用 JDK 14+ 的 Switch 表达式，代码极简。
        // 如果你的策略非常多，这里也可以升级为 Spring 自动注入的 List<FlowControllerProvider> 来彻底解耦。
        return switch (strategy) {
            case TOKEN_BUCKET_RATE_LIMIT -> new TokenBucketFlowController(config);
            case NONE -> null;
            default -> throw new UnsupportedOperationException("不支持的限流策略: " + strategy);
        };
    }
}
