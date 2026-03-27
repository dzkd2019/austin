package com.java3y.austin.handler.script.impl.mock;

import com.java3y.austin.common.dto.account.sms.YunPianSmsAccount;
import com.java3y.austin.handler.domain.sms.SmsParam;
import com.java3y.austin.handler.script.SmsScript;
import com.java3y.austin.handler.script.impl.mock.utils.MockAssembleUtils;
import com.java3y.austin.support.domain.SmsRecord;
import com.java3y.austin.support.utils.AccountUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

@Component("MockYunPianScript")
@Slf4j
@Profile("test")
public class MockYunPianScript implements SmsScript {
    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    @Override
    public List<SmsRecord> send(SmsParam smsParam) {
        YunPianSmsAccount yunPianSmsAccount = new YunPianSmsAccount();
        try {
            Thread.sleep(50 + random.nextInt(50));
            log.info("调用YunPian发送短信接口成功");
        } catch (InterruptedException e) {
            log.error("模拟YunPian发送短信过程中被中断");
            Thread.currentThread().interrupt();
        }
        return List.of(new SmsRecord());
    }

    @Override
    public List<SmsRecord> pull(Integer id) {
        try {
            Thread.sleep(50 + random.nextInt(50));
            log.info("调用YunPian拉取回执接口成功");
        } catch (InterruptedException e) {
            log.error("模拟YunPian拉取回执过程中被中断");
            Thread.currentThread().interrupt();
        }
        return List.of();
    }
}
