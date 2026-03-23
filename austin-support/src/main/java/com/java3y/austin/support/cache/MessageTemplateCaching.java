package com.java3y.austin.support.cache;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.java3y.austin.support.config.ThreadPoolExecutorShutdownDefinition;
import com.java3y.austin.support.dao.MessageTemplateDao;
import com.java3y.austin.support.domain.MessageTemplate;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class MessageTemplateCaching {
    private final MessageTemplateDao messageTemplateDao;

    private final ExecutorService cacheLoaderExecutor = Executors.newVirtualThreadPerTaskExecutor();

    private AsyncLoadingCache<Long, MessageTemplate> messageTemplateCache;

    private final ThreadPoolExecutorShutdownDefinition threadPoolExecutorShutdownDefinition;

    private static final MessageTemplate NULL_MESSAGE_TEMPLATE = new MessageTemplate();

    public MessageTemplateCaching(MessageTemplateDao messageTemplateDao, ThreadPoolExecutorShutdownDefinition threadPoolExecutorShutdownDefinition) {
        this.messageTemplateDao = messageTemplateDao;
        this.threadPoolExecutorShutdownDefinition = threadPoolExecutorShutdownDefinition;
    }

    @PostConstruct
    public void init() {
        threadPoolExecutorShutdownDefinition.registryExecutor(cacheLoaderExecutor);
        this.messageTemplateCache = Caffeine.newBuilder()
                // 1. 基础容量控制
                .maximumSize(5000)
                // 2. 被动一致性控制：写入后 10 分钟自动过期
                .expireAfterWrite(10, TimeUnit.MINUTES)
                // 3. 【核心配置】：指定使用虚拟线程池去执行数据库查询！
                .executor(cacheLoaderExecutor)
                // 4. 定义如何从数据库加载数据
                .buildAsync(templateId -> {
                    log.info("缓存未命中，正由虚拟线程去数据库加载模板 ID: {}", templateId);
                    // 这里发生数据库 I/O 阻塞，但毫无关系，因为是虚拟线程！
                    return messageTemplateDao.findById(templateId).orElse(null);
                });
    }

    /**
     * 存在竞态条件
     * T1: get() 返回 null
     * T2: get() 返回 null
     * T1: put(NULL_MESSAGE_TEMPLATE)
     * T2: put(NULL_MESSAGE_TEMPLATE)
     * T3: 数据库更新，模板存在了
     * 但缓存中仍是 NULL_MESSAGE_TEMPLATE，直到过期
     * 建议：使用 Caffeine 的 refresh 机制或更完善的缓存策略
     * todo 避免竞态条件
     */
    public Optional<MessageTemplate> getMessageTemplate(Long id) {
        MessageTemplate template = messageTemplateCache.get(id).join();
        if (template == null) {
            // 为了防止缓存穿透，将 null 值也缓存起来（使用一个特殊的 NULL_MESSAGE_TEMPLATE 对象）
            messageTemplateCache.synchronous().put(id, NULL_MESSAGE_TEMPLATE);
        } else if(template == NULL_MESSAGE_TEMPLATE) {
            // 如果缓存中是 NULL_MESSAGE_TEMPLATE，说明数据库中确实没有这个模板，直接返回 Optional.empty()
            return Optional.empty();
        }
        return Optional.ofNullable(template);
    }

    public void removeMessageTemplate(Long id) {
        messageTemplateCache.synchronous().invalidate(id);
    }
}
