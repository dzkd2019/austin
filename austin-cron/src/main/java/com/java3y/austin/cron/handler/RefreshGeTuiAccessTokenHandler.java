package com.java3y.austin.cron.handler;

import cn.hutool.core.text.CharSequenceUtil;
import com.alibaba.fastjson2.JSON;
import com.java3y.austin.common.constant.CommonConstant;
import com.java3y.austin.common.dto.account.GeTuiAccount;
import com.java3y.austin.common.enums.ChannelType;
import com.java3y.austin.support.dao.ChannelAccountDao;
import com.java3y.austin.support.domain.ChannelAccount;
import com.java3y.austin.support.utils.AccessTokenUtils;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.concurrent.Executors;


/**
 * 刷新个推的token
 * <p>
 * <a href="https://docs.getui.com/getui/server/rest_v2/token/">...</a>
 *
 * @author 3y
 */
@Service
@Slf4j
public class RefreshGeTuiAccessTokenHandler {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private ChannelAccountDao channelAccountDao;

    @Autowired
    private AccessTokenUtils accessTokenUtils;


    /**
     * 每小时请求一次接口刷新（以防失效)
     */
    @XxlJob("refreshGeTuiAccessTokenJob")
    public void execute() {
        log.info("refreshGeTuiAccessTokenJob#execute!");

        List<ChannelAccount> accountList = channelAccountDao.findAllByIsDeletedEqualsAndSendChannelEquals(CommonConstant.FALSE, ChannelType.PUSH.getCode());
        if(CollectionUtils.isEmpty(accountList)) {
            log.info("没有需要刷新的个推账号，任务结束！");
            return;
        }

        // 2. 必须使用 Executors.newVirtualThreadPerTaskExecutor() 创建针对本次任务的专属作用域
        // try-with-resources 会在代码块结束时自动调用 close()，
        // 这将阻塞 XXL-JOB 主线程，直到所有 submit 进去的虚拟线程全部执行完毕！
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {

            for (ChannelAccount channelAccount : accountList) {

                // 3. 将单条数据的【完整业务流】作为一个整体丢进虚拟线程
                executor.submit(() -> {
                    try {
                        // JSON 解析
                        GeTuiAccount account = JSON.parseObject(channelAccount.getAccountConfig(), GeTuiAccount.class);

                        // 【核心纠正】：HTTP 网络请求 (I/O) 现在由并发的虚拟线程去执行！
                        // 如果有 100 个账号，这 100 个 HTTP 请求将同时发出，耗时取决于最慢的那一个！
                        String accessToken = accessTokenUtils.getAccessToken(
                                ChannelType.PUSH.getCode(), channelAccount.getId().intValue(), account, true);

                        if (CharSequenceUtil.isNotBlank(accessToken)) {
                            // Redis 写入 (I/O)
                            redisTemplate.opsForValue().set(
                                    ChannelType.PUSH.getAccessTokenPrefix() + channelAccount.getId(), accessToken);
                        }
                    } catch (Exception e) {
                        // 记录单条数据的失败，不影响其他账号的刷新
                        log.error("refreshGeTuiAccessTokenJob 刷新账号 {} 失败!", channelAccount.getId(), e);
                    }
                });
                // 注意：这里绝对没有 .get() ！！
            }

        } // XXL-JOB 主线程会在此处停顿，安静地等待所有虚拟线程干完活儿...

        log.info("refreshGeTuiAccessTokenJob#execute end! 所有账号刷新完毕。");

//        SupportThreadPoolConfig.getPendingSingleThreadPool().execute(() -> {
//            List<ChannelAccount> accountList = channelAccountDao.findAllByIsDeletedEqualsAndSendChannelEquals(CommonConstant.FALSE, ChannelType.PUSH.getCode());
//            for (ChannelAccount channelAccount : accountList) {
//                GeTuiAccount account = JSON.parseObject(channelAccount.getAccountConfig(), GeTuiAccount.class);
//                String accessToken = accessTokenUtils.getAccessToken(ChannelType.PUSH.getCode(), channelAccount.getId().intValue(), account, true);
//                if (CharSequenceUtil.isNotBlank(accessToken)) {
//                    redisTemplate.opsForValue().set(ChannelType.PUSH.getAccessTokenPrefix() + channelAccount.getId(), accessToken);
//                }
//            }
//        });
    }


}
