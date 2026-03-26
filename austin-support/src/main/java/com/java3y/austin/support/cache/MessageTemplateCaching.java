package com.java3y.austin.support.cache;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.java3y.austin.support.config.ThreadPoolExecutorShutdownDefinition;
import com.java3y.austin.support.dao.MessageTemplateDao;
import com.java3y.austin.support.domain.MessageTemplate;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Optional;
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
                // 数据库无记录时返回 NULL_MESSAGE_TEMPLATE 哨兵对象（非 null）
                // 这样 Caffeine 会将"无此模板"这一结果也缓存起来，防止缓存穿透，
                // 且无需在调用方额外写入，从根本上消除了并发写入时的竞态条件。
                .buildAsync(templateId -> {
                    log.info("缓存未命中，正由虚拟线程去数据库加载模板 ID: {}", templateId);
                    // 这里发生数据库 I/O 阻塞，但毫无关系，因为是虚拟线程！
                    return messageTemplateDao.findById(templateId).orElse(NULL_MESSAGE_TEMPLATE);
                });
    }

    /**
     * 获取消息模板（带缓存穿透防护）
     *
     * <p>loader 已将 DB 无记录的情况映射为 NULL_MESSAGE_TEMPLATE 哨兵，
     * Caffeine 会将该哨兵值正常缓存，后续相同 ID 的请求直接命中缓存，
     * 不再穿透到数据库，也不存在并发写入时的竞态条件。
     */
    public Optional<MessageTemplate> getMessageTemplate(Long id) {
        MessageTemplate template = messageTemplateCache.get(id).join();
        if (template == NULL_MESSAGE_TEMPLATE) {
            return Optional.empty();
        }
        return Optional.ofNullable(template);
    }

    public void removeMessageTemplate(Long id) {
        messageTemplateCache.synchronous().invalidate(id);
    }

    public void removeMessageTemplate(Collection<Long> ids) {
        messageTemplateCache.synchronous().invalidateAll(ids);
    }

    public void put(MessageTemplate messageTemplate) {
        messageTemplateCache.synchronous().put(messageTemplate.getId(), messageTemplate);
    }

    public boolean contains(Long id) {
        return messageTemplateCache.asMap().containsKey(id);
    }
}
