package com.java3y.austin.support.cache;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.java3y.austin.support.config.ThreadPoolExecutorShutdownDefinition;
import com.java3y.austin.support.dao.ChannelAccountDao;
import com.java3y.austin.support.domain.ChannelAccount;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class ChannelAccountCaching {
    private final ChannelAccountDao channelAccountDao;

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    private final ThreadPoolExecutorShutdownDefinition shutdownDefinition;

    private AsyncLoadingCache<Long, ChannelAccount> channelAccountCache;

    private static final ChannelAccount NULL_ACCOUNT = new ChannelAccount();

    public ChannelAccountCaching(ChannelAccountDao channelAccountDao, ThreadPoolExecutorShutdownDefinition shutdownDefinition) {
        this.channelAccountDao = channelAccountDao;
        this.shutdownDefinition = shutdownDefinition;
    }

    @PostConstruct
    public void init() {
        shutdownDefinition.registryExecutor(executor);

        channelAccountCache = Caffeine.newBuilder()
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .maximumSize(1000)
                .executor(executor)
                .buildAsync(accountId -> {
                    log.info("缓存未命中，正由虚拟线程去数据库加载渠道账号 ID: {}", accountId);
                    return channelAccountDao.findById(accountId).orElse(NULL_ACCOUNT);
                });
    }

    public Optional<ChannelAccount> get(Long accountId) {
        ChannelAccount account = channelAccountCache.get(accountId).join();
        if (account == NULL_ACCOUNT) {
            return Optional.empty();
        }

        return Optional.of(account);
    }

    public void remove(Long accountId) {
        channelAccountCache.synchronous().invalidate(accountId);
    }

    public void removeAll(List<Long> accountIds) {
        channelAccountCache.synchronous().invalidateAll(accountIds);
    }

    public boolean contains(Long accountId) {
        return channelAccountCache.asMap().containsKey(accountId);
    }
}
