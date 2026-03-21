package com.java3y.austin.cron.handler;

import com.dtp.core.thread.DtpExecutor;
import com.java3y.austin.cron.config.CronAsyncThreadPoolConfig;
import com.java3y.austin.cron.service.TaskHandler;
import com.java3y.austin.support.utils.ThreadPoolUtils;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


/**
 * 后台提交的定时任务处理类
 *
 * @author 3y
 */
@Service
@Slf4j
public class CronTaskHandler {

    @Autowired
    private TaskHandler taskHandler;

    @Autowired
    private ThreadPoolUtils threadPoolUtils;
    private final DtpExecutor dtpExecutor = CronAsyncThreadPoolConfig.getXxlCronExecutor();

    /**
     * 处理后台的 austin 定时任务消息
     */
    @XxlJob("austinJob")
    public void execute() {
        log.info("CronTaskHandler#execute messageTemplateId:{} cron exec!", XxlJobHelper.getJobParam());
        //threadPoolUtils.register(dtpExecutor);

        Long messageTemplateId = Long.valueOf(XxlJobHelper.getJobParam());

//        ThreadPoolUtils.getVirtualExecutorService().execute(() -> taskHandler.handle(messageTemplateId));
//        dtpExecutor.execute(() -> taskHandler.handle(messageTemplateId));

        // 直接同步执行，阻塞 XXL-JOB 线程，直到文件读取并入队完毕！
        taskHandler.handle(messageTemplateId);
    }

}
