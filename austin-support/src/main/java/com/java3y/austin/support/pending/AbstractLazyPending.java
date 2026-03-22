package com.java3y.austin.support.pending;

import cn.hutool.core.collection.CollUtil;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.java3y.austin.support.utils.ThreadPoolUtils;
import jakarta.annotation.PostConstruct;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 延迟消费 阻塞队列-消费者和生产者实现
 *
 * @author 3y
 */
@Slf4j
@Data
public abstract class AbstractLazyPending<T> {

    /**
     * 子类构造方法必须初始化该参数
     */
    protected PendingParam<T> pendingParam;

    /**
     * 批量装载任务
     */
    private List<T> tasks = new ArrayList<>();

    /**
     * 上次执行的时间
     */
    private Long lastHandleTime = System.currentTimeMillis();

    /**
     * 是否终止线程
     */
    private volatile Boolean stop = false;

    private final Semaphore inFlightBatches = new Semaphore(100);
    /**
     * 单线程消费 阻塞队列的数据
     */
    @PostConstruct
    public void initConsumePending() {
        Thread.ofVirtual().name("Pending Thread").start(() -> {

            while(!Boolean.TRUE.equals(this.stop) || CollUtil.isNotEmpty(tasks) || !pendingParam.getQueue().isEmpty()) {
                try {
                    // 1. 【修复 take() 的假死问题】
                    // 使用带有超时的 poll。假设最大等待时间是 pendingParam.getTimeThreshold()
                    // 这样即使没新数据，线程也会周期性醒来，去执行下面的 dataReady() 时间判断！
                    T obj = pendingParam.getQueue().poll(
                            pendingParam.getTimeThreshold(), TimeUnit.MILLISECONDS);

                    if (obj != null) {
                        tasks.add(obj);
                    }

                    // 2. 核心发送条件判断
                    if (CollUtil.isNotEmpty(tasks) && dataReady()) {
                        List<T> taskRef = tasks;
                        tasks = Lists.newArrayList();
                        lastHandleTime = System.currentTimeMillis();

                        // 3. 【修复 Semaphore 失效问题】
                        // 主线程在此处阻塞获取许可。如果下游满了，主线程会卡在这里，从而停止从 queue 中 poll 数据
                        // todo 当主线程阻塞达到一定时间后，应有报警或其他处理机制，防止死锁。使用 tryAcquire()方法
                        inFlightBatches.acquire();

                        // 提交异步任务
                        ThreadPoolUtils.getVirtualExecutorService().execute(() -> {
                            try {
                                // 真正的耗时网络 I/O 都在这个虚拟线程里执行
                                this.handle(taskRef);
                            } catch (Exception e) {
                                log.error("处理定时发送任务的csv文件时出现错误，异常: {}", e.getMessage(), e);
                            } finally {
                                // 【必须写在这里！】
                                // 只有当耗时任务真干完了（或报错了），才由该虚拟线程归还许可！
                                inFlightBatches.release();
                            }
                        });
                    }

                } catch (InterruptedException e) {
                    log.info("Pending Thread 收到中断信号，准备退出...");
                    Thread.currentThread().interrupt(); // 恢复中断标志位
                    break; // 安全退出死循环
                } catch (Exception e) {
                    log.error("Pending Thread 轮询过程中发生异常: {}", e.getMessage(), e);
                }
            }

            log.info("Pending Thread 已安全停止。");
        });
    }

    /**
     * 1. 数量超限
     * 2. 时间超限
     *
     * @return
     */
    private boolean dataReady() {
        return tasks.size() >= pendingParam.getNumThreshold() ||
                (System.currentTimeMillis() - lastHandleTime >= pendingParam.getTimeThreshold());
    }

    /**
     * 将元素放入阻塞队列中
     *
     * @param t
     */
    public void pending(T t) {
        try {
            pendingParam.getQueue().put(t);
        } catch (InterruptedException e) {
            log.error("Pending#pending error:{}", Throwables.getStackTraceAsString(e));
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 消费阻塞队列元素时的方法
     *
     * @param t
     */
    public void handle(List<T> t) {
        if (t.isEmpty()) {
            return;
        }
        try {
            doHandle(t);
        } catch (Exception e) {
            log.error("Pending#handle failed:{}", Throwables.getStackTraceAsString(e));
        }
    }

    /**
     * 处理阻塞队列的元素 真正方法
     *
     * @param list
     */
    public abstract void doHandle(List<T> list);

}
