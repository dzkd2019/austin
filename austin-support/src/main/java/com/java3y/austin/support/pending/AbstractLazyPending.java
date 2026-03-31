package com.java3y.austin.support.pending;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.IdUtil;
import com.google.common.base.Throwables;
import com.java3y.austin.support.constans.MdcConstant;
import com.java3y.austin.support.utils.MdcUtil;
import com.java3y.austin.support.utils.ThreadPoolUtils;
import com.java3y.austin.support.vo.CrowdInfoVo;
import jakarta.annotation.PostConstruct;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private List<T> tasks;

    /**
     * 上次执行的时间
     */
    private Long lastHandleTime = System.currentTimeMillis();

    /**
     * 是否终止线程
     */
    private volatile Boolean stop = false;

    /**
     * 单线程消费 阻塞队列的数据
     */
    @PostConstruct
    public void initConsumePending() {
        tasks = new ArrayList<>(pendingParam.getNumThreshold());
        String bizId = IdUtil.fastSimpleUUID();

        Thread.ofVirtual().name("Pending Thread").start(() -> {

            while (!Boolean.TRUE.equals(this.stop) || CollUtil.isNotEmpty(tasks) || !pendingParam.getQueue().isEmpty()) {
                try {
                    // 1. 使用带有超时的 poll，避免 take() 导致的假死问题
                    T obj = pendingParam.getQueue().poll(
                            pendingParam.getTimeThreshold(), TimeUnit.MILLISECONDS);

                    if (obj != null) {
                        tasks.add(obj);
                    }

                    // 2. 核心发送条件判断
                    if (CollUtil.isNotEmpty(tasks) && dataReady()) {
                        List<T> taskRef = tasks;
                        tasks = new ArrayList<>(pendingParam.getNumThreshold());
                        lastHandleTime = System.currentTimeMillis();

                        Map<String, String> mdcContext = new HashMap<>();
                        T ref = taskRef.getFirst();
                        if(ref instanceof CrowdInfoVo crowd){
                            mdcContext.put(MdcConstant.MDC_TEMPLATE_ID, crowd.getMessageTemplateId().toString());
                            mdcContext.put(MdcConstant.XXL_JOB_ID, String.valueOf(crowd.getXxlJobId()));
                        }
                        mdcContext.put(MdcConstant.MDC_TRACE_ID, IdUtil.fastSimpleUUID());
                        mdcContext.put(MdcConstant.MDC_BUSINESS_ID, bizId);
                        // 提交异步任务；全局限流由 SendMqAction 中的 MqRateLimiter 统一管控
                        ThreadPoolUtils.getVirtualExecutorService().execute(MdcUtil.wrap(mdcContext, () -> {
                            try {
                                this.handle(taskRef);
                            } catch (Exception e) {
                                log.error("处理定时发送任务时出现错误", e);
                            }
                        }));
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
            Thread.currentThread().interrupt();
            log.error("Pending#pending interrupted while enqueue task", e);
            throw new IllegalStateException("enqueue pending task interrupted", e);
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

        doHandle(t);
    }

    /**
     * 处理阻塞队列的元素 真正方法
     *
     * @param list
     */
    public abstract void doHandle(List<T> list);

}
