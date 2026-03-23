package com.java3y.austin.support.utils;

import com.java3y.austin.support.config.ThreadPoolExecutorShutdownDefinition;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 线程池工具类
 *
 * @author 3y
 */
@Component
public class ThreadPoolUtils {

    private static final String SOURCE_NAME = "austin";
    @Autowired
    private ThreadPoolExecutorShutdownDefinition shutdownDefinition;

    private static final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

    @PostConstruct
    public void init() {
        shutdownDefinition.registryExecutor(executorService);
    }

    public static ExecutorService getVirtualExecutorService() {
        return executorService;
    }
}
