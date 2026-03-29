package com.java3y.austin.handler.backpressure;

import com.java3y.austin.common.constant.AustinConstant;
import com.java3y.austin.support.utils.GroupIdMappingUtils;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * 虚拟线程背压管理器 - 支持每个 groupId 独立的水位配置
 *
 * <p>设计要点：
 * <ul>
 *   <li>每个 Kafka Listener（groupId）有独立的水位线</li>
 *   <li>支持动态调整单个 groupId 的水位</li>
 *   <li>提供全局默认水位作为 fallback</li>
 *   <li>使用 CAS 原子操作防止重复 pause/resume</li>
 * </ul>
 *
 * @author 3y
 */
@Slf4j
@Component
public class VirtualThreadBackPressureManager {
    // ==================== 全局默认水位（作为 fallback）====================

    private volatile WaterMarkConfig defaultWaterMark = new WaterMarkConfig(100000, 50000);
    // ==================== 存储 ====================

    /**
     * groupId -> GroupContext
     * 包含计数器、暂停状态、容器引用、独立水位配置
     */
    private final Map<String, GroupContext> groupContexts = new ConcurrentHashMap<>();
    private final KafkaListenerEndpointRegistry registry;
    private final StringRedisTemplate redisTemplate;

    // ==================== 构造函数 =====================
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public VirtualThreadBackPressureManager(KafkaListenerEndpointRegistry registry, StringRedisTemplate redisTemplate) {
        this.registry = registry;
        this.redisTemplate = redisTemplate;
    }

    @PostConstruct
    public void init() {
        // 提前预热上下文。即使 container 暂时为 null 也无妨，
        // 当真正消费发生时，getOrCreateGroupContext 会尝试再次捕获 container
        GroupIdMappingUtils.getAllGroupIds().forEach(this::getOrCreateGroupContext);
        log.info("虚拟线程背压管理器初始化完成，全局默认水位: High={}, Low={}",
                defaultWaterMark.highWaterMark(), defaultWaterMark.lowWaterMark());
    }

    // ==================== 核心方法 =====================

    /**
     * 增加计数并检查是否需要暂停
     */
    public void incrementAndCheckPause(String groupId) {
        GroupContext context = getOrCreateGroupContext(groupId);
        int current = context.inFlightCount.incrementAndGet();
        int highWaterMark = context.getHighWaterMark();
        // 【CAS 控制】：只有首次超过高水位时才执行 pause()

        log.debug("Group [{}] pendingCount={}, highWaterMark={}", groupId, current, highWaterMark);
        if (current >= highWaterMark && context.isPaused.compareAndSet(false, true)) {
            MessageListenerContainer container = context.getContainer();
            if (container != null) {
                log.warn("Group [{}] 触及高水位 ({} / {})，正式下达暂停消费指令",
                        groupId, current, highWaterMark);
                container.pause();
                redisTemplate.convertAndSend(AustinConstant.REDIS_BACKPRESSURE_TOPIC, groupId + ":pause");
            }
        }
    }

    /**
     * 减少计数并检查是否需要恢复
     */
    public void decrementAndCheckResume(String groupId) {
        GroupContext context = getOrCreateGroupContext(groupId);
        int current = context.inFlightCount.decrementAndGet();
        int lowWaterMark = context.getLowWaterMark();
        // 【CAS 控制】：只有首次回落到低水位时才执行 resume()
        if (current <= lowWaterMark && context.isPaused.compareAndSet(true, false)) {
            MessageListenerContainer container = context.getContainer();
            if (container != null) {
                log.info("Group [{}] 水位回落至低水位 ({} / {})，恢复拉取消费",
                        groupId, current, lowWaterMark);
                container.resume();
                redisTemplate.convertAndSend(AustinConstant.REDIS_BACKPRESSURE_TOPIC, groupId + ":resume");
            }
        }
    }
    // ==================== 水位配置 API =====================

    /**
     * 为指定 groupId 设置水位
     *
     * @param id       Channel_id
     * @param highWaterMark 高水位
     * @param lowWaterMark  低水位
     */
    public void setWaterMark(String id, int highWaterMark, int lowWaterMark) {
        validateWaterMarks(highWaterMark, lowWaterMark);

        List<String> adaptedGroupIds = adaptGroupId(id);
        if(adaptedGroupIds == null || adaptedGroupIds.isEmpty()) {
            log.error("无效的 groupId [{}]，无法设置水位", id);
            return;
        }

        for (String groupId : adaptedGroupIds) {
            GroupContext context = getOrCreateGroupContext(groupId);
            context.setCustomConfig(new WaterMarkConfig(highWaterMark, lowWaterMark));

            log.info("设置 Group [{}] 水位: 高水位={}, 低水位={}",
                    groupId, highWaterMark, lowWaterMark);

            // 检查是否需要立即恢复（当前水位已低于新的低水位）
            tryResumeIfBelowLowWaterMark(context, groupId);
        }
    }

    /**
     * 为指定 groupId 设置水位（使用 WaterMarkConfig）
     */
    public void setWaterMark(String groupId, WaterMarkConfig config) {
        setWaterMark(groupId, config.highWaterMark(), config.lowWaterMark());
    }

    /**
     * 批量设置多个 groupId 的水位
     *
     * @param waterMarkConfigs groupId -> WaterMarkConfig 的映射
     */
    public void setWaterMarks(Map<String, WaterMarkConfig> waterMarkConfigs) {
        if (waterMarkConfigs == null || waterMarkConfigs.isEmpty()) {
            return;
        }
        waterMarkConfigs.forEach(this::setWaterMark);
    }

    /**
     * 移除指定 groupId 的独立水位配置，恢复使用全局默认水位
     *
     * @param groupId Kafka 消费者组 ID
     */
    public void resetToDefaultWaterMark(String groupId) {
        GroupContext context = groupContexts.get(groupId);
        if (context != null && !context.isUsingDefaultWaterMark()) {
            context.resetToDefault();
            log.info("Group [{}] 已恢复使用全局默认水位", groupId);
            tryResumeIfBelowLowWaterMark(context, groupId);
        }
    }

    /**
     * 更新全局默认水位（影响所有未单独配置的 groupId）
     */
    public void updateDefaultWaterMarks(int highWaterMark, int lowWaterMark) {
        validateWaterMarks(highWaterMark, lowWaterMark);

        log.info("更新全局默认水位: 高水位 {} -> {}, 低水位 {} -> {}",
                this.defaultWaterMark.highWaterMark(), highWaterMark,
                this.defaultWaterMark.lowWaterMark(), lowWaterMark);

        this.defaultWaterMark = new WaterMarkConfig(highWaterMark, lowWaterMark);

        // 遍历所有使用默认水位的 context，触发可能的恢复
        groupContexts.forEach((groupId, context) -> {
            if (context.isUsingDefaultWaterMark()) {
                context.resetToDefault();
                tryResumeIfBelowLowWaterMark(context, groupId);
            }
        });
    }
    // ==================== 查询方法 =====================

    /**
     * 获取指定 groupId 的当前水位配置
     */
    public WaterMarkConfig getWaterMarkConfig(String groupId) {
        GroupContext context = groupContexts.get(groupId);
        if (context == null) {
            return new WaterMarkConfig(defaultWaterMark.highWaterMark(), defaultWaterMark.lowWaterMark());
        }
        return new WaterMarkConfig(context.getHighWaterMark(), context.getLowWaterMark());
    }

    /**
     * 获取指定 groupId 的当前飞行中任务数
     */
    public int getInFlightCount(String groupId) {
        GroupContext context = groupContexts.get(groupId);
        return context != null ? context.inFlightCount.get() : 0;
    }

    /**
     * 获取指定 groupId 是否处于暂停状态
     */
    public boolean isPaused(String groupId) {
        GroupContext context = groupContexts.get(groupId);
        return context != null && context.isPaused.get();
    }

    public Set<String> getCustomizedGroupIds() {
        return groupContexts.entrySet()
                .stream()
                .filter(entry -> !entry.getValue().isUsingDefaultWaterMark())
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }
    // ==================== 私有方法 =====================

    private GroupContext getOrCreateGroupContext(String groupId) {

        return groupContexts.computeIfAbsent(groupId, k -> {
            // 将 registry 和一个"实时获取最新全局水位的函数(Supplier)" 传给内部类
            return new GroupContext(k, registry, () -> this.defaultWaterMark);
        });

    }

    private void validateWaterMarks(int highWaterMark, int lowWaterMark) {
        if (lowWaterMark <= 0) {
            throw new IllegalArgumentException("低水位必须大于 0，当前值: " + lowWaterMark);
        }
        if (highWaterMark <= lowWaterMark) {
            throw new IllegalArgumentException(
                    String.format("高水位(%d)必须严格大于低水位(%d)", highWaterMark, lowWaterMark));
        }
    }

    /**
     * 检查并尝试恢复消费（如果当前水位低于低水位）
     *
     * <p>触发场景：
     * <ul>
     *   <li>调用 setWaterMark() 设置更宽松的水位后</li>
     *   <li>调用 updateDefaultWaterMarks() 更新全局默认水位后</li>
     *   <li>调用 resetToDefaultWaterMark() 重置为默认水位后</li>
     * </ul>
     *
     * @param context 消费者组上下文
     * @param groupId 消费者组 ID（用于日志）
     */
    private void tryResumeIfBelowLowWaterMark(GroupContext context, String groupId) {
        // 获取当前计数
        int current = context.inFlightCount.get();
        // 获取该 context 的低水位（可能是独立配置或默认值）
        int lowWaterMark = context.getLowWaterMark();

        // 条件：当前计数 <= 低水位 且 当前是暂停状态
        // 使用 CAS 确保只恢复一次
        if (current <= lowWaterMark && context.isPaused.compareAndSet(true, false)) {
            MessageListenerContainer container = context.getContainer();
            if (container != null) {
                log.info("因水位阈值放宽，Group [{}] 提前恢复消费 ({} / {})",
                        groupId, current, lowWaterMark);
                container.resume();
            }
        }
    }

    private List<String> adaptGroupId(String groupId) {
        String[] split = groupId.split("\\.");
        if (split.length == 2) {
            return List.of(groupId);
        } else if (split.length > 2) {
            // 格式非法，返回空列表而非 null，避免调用方 NPE
            return List.of();
        }

        return GroupIdMappingUtils.getGroupIdByChannel(groupId);
    }
    // ==================== 内部类 =====================

    /**
     * 每个消费者组的上下文
     */
    private static class GroupContext {
        final String groupId;
        final AtomicInteger inFlightCount = new AtomicInteger(0);
        final AtomicBoolean isPaused = new AtomicBoolean(false);

        // 显式持有的外部依赖
        private final KafkaListenerEndpointRegistry registry;
        // 一个能够实时获取外部最新全局水位的函数指针
        private final Supplier<WaterMarkConfig> defaultWaterMarkSupplier;

        private volatile MessageListenerContainer container;
        private volatile WaterMarkConfig customConfig = null;

        GroupContext(String groupId,
                     KafkaListenerEndpointRegistry registry,
                     Supplier<WaterMarkConfig> defaultWaterMarkSupplier) {
            this.groupId = groupId;
            this.registry = registry;
            this.defaultWaterMarkSupplier = defaultWaterMarkSupplier;
            tryBindContainer();
        }

        MessageListenerContainer getContainer() {
            if (container == null) {
                tryBindContainer();
            }
            return container;
        }

        private void tryBindContainer() {
            registry.getListenerContainers().stream()
                    .filter(c -> groupId.equals(c.getGroupId()))
                    .findFirst()
                    .ifPresent(c -> this.container = c);
        }

        /**
         * 动态路由：优先使用自定义配置，否则通过 Supplier 实时读取外部最新的全局配置
         */
        WaterMarkConfig getConfig() {
            WaterMarkConfig cfg = customConfig;
            // 当 customConfig 为空时，调用 Supplier.get() 瞬间拿到外部最新的 defaultWaterMark
            if (cfg == null) cfg = defaultWaterMarkSupplier.get();
            if (cfg == null) {
                throw new IllegalStateException("Default watermark config is null for group: " + groupId);
            }
            return cfg;
        }

        void resetToDefault() {
            this.customConfig = null;
        }

        int getHighWaterMark() {
            return getConfig().highWaterMark();
        }

        int getLowWaterMark() {
            return getConfig().lowWaterMark();
        }

        boolean isUsingDefaultWaterMark() {
            return customConfig == null;
        }

        void setCustomConfig(WaterMarkConfig config) {
            this.customConfig = config;
        }
    }
}