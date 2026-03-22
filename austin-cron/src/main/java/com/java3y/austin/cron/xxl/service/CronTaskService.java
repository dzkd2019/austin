package com.java3y.austin.cron.xxl.service;

import com.java3y.austin.common.vo.BasicResultVO;
import com.java3y.austin.cron.xxl.entity.XxlJobGroup;
import com.java3y.austin.cron.xxl.entity.XxlJobInfo;

/**
 * 定时任务服务
 *
 * @author 3y
 */
public interface CronTaskService {


    /**
     * 新增/修改 定时任务
     *
     * @return 新增时返回任务Id，修改时无返回
     */
    BasicResultVO<Integer> saveCronTask(XxlJobInfo xxlJobInfo);

    /**
     * 删除定时任务
     *
     * @return BasicResultVO
     */
    BasicResultVO<Void> deleteCronTask(Integer taskId);

    /**
     * 启动定时任务
     *
     * @return BasicResultVO
     */
    BasicResultVO<Void> startCronTask(Integer taskId);


    /**
     * 暂停定时任务
     *
     * @return BasicResultVO
     */
    BasicResultVO<Void> stopCronTask(Integer taskId);


    /**
     * 得到执行器Id
     *
     * @return BasicResultVO
     */
    BasicResultVO<Integer> getGroupId(String appName, String title);

    /**
     * 创建执行器
     *
     * @return BasicResultVO
     */
    BasicResultVO<Void> createGroup(XxlJobGroup xxlJobGroup);

}
