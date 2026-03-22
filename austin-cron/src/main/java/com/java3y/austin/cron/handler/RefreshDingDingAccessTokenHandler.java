package com.java3y.austin.cron.handler;

import cn.hutool.core.text.CharSequenceUtil;
import com.alibaba.fastjson2.JSON;
import com.java3y.austin.common.constant.CommonConstant;
import com.java3y.austin.common.dto.account.DingDingWorkNoticeAccount;
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
 * 刷新钉钉的access_token
 * <p>
 * <a href="https://open.dingtalk.com/document/orgapp-server/obtain-orgapp-token">...</a>
 *
 * @author 3y
 */
@Service
@Slf4j
public class RefreshDingDingAccessTokenHandler {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private ChannelAccountDao channelAccountDao;

    @Autowired
    private AccessTokenUtils accessTokenUtils;


    /**
     * 每小时请求一次接口刷新（以防失效)
     */
    @XxlJob("refreshAccessTokenJob")
    public void execute() {
        log.info("refreshDingDingAccessTokenJob#execute!");

        List<ChannelAccount> accountList = channelAccountDao.findAllByIsDeletedEqualsAndSendChannelEquals(CommonConstant.FALSE, ChannelType.DING_DING_WORK_NOTICE.getCode());

        if (CollectionUtils.isEmpty(accountList)) {
            log.info("没有需要刷新的钉钉账号，任务结束！");
            return;
        }

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (ChannelAccount channelAccount : accountList) {
                executor.submit(() -> {
                    try {
                        DingDingWorkNoticeAccount account = JSON.parseObject(channelAccount.getAccountConfig(), DingDingWorkNoticeAccount.class);
                        String accessToken = accessTokenUtils.getAccessToken(ChannelType.DING_DING_WORK_NOTICE.getCode(), channelAccount.getId().intValue(), account, true);

                        if (CharSequenceUtil.isNotBlank(accessToken)) {
                            redisTemplate.opsForValue().set(ChannelType.DING_DING_WORK_NOTICE.getAccessTokenPrefix() + channelAccount.getId(), accessToken);
                        }
                    } catch (Exception e) {
                        log.error("refreshDingDingAccessTokenJob 刷新账号 {} 失败!", channelAccount.getId(), e);
                    }
                });
            }
        }
        log.info("refreshDingDingAccessTokenJob#execute end!");

        //        SupportThreadPoolConfig.getPendingSingleThreadPool().execute(() -> {
//            List<ChannelAccount> accountList = channelAccountDao.findAllByIsDeletedEqualsAndSendChannelEquals(CommonConstant.FALSE, ChannelType.DING_DING_WORK_NOTICE.getCode());
//            for (ChannelAccount channelAccount : accountList) {
//                DingDingWorkNoticeAccount account = JSON.parseObject(channelAccount.getAccountConfig(), DingDingWorkNoticeAccount.class);
//                String accessToken = accessTokenUtils.getAccessToken(ChannelType.DING_DING_WORK_NOTICE.getCode(), channelAccount.getId().intValue(), account, true);
//                if (CharSequenceUtil.isNotBlank(accessToken)) {
//                    redisTemplate.opsForValue().set(ChannelType.DING_DING_WORK_NOTICE.getAccessTokenPrefix() + channelAccount.getId(), accessToken);
//                }
//            }
//        });
    }
}


