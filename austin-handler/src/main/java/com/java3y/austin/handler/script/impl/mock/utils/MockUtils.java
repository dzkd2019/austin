package com.java3y.austin.handler.script.impl.mock.utils;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import com.java3y.austin.common.dto.account.sms.SmsAccount;
import com.java3y.austin.common.enums.SmsStatus;
import com.java3y.austin.handler.domain.sms.SmsParam;
import com.java3y.austin.support.domain.SmsRecord;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class MockUtils {
    public static List<SmsRecord> assembleSendSmsRecord(SmsParam smsParam, SmsAccount smsAccount) {

        List<SmsRecord> smsRecordList = new ArrayList<>();


        for (String phone : smsParam.getPhones()) {


            SmsRecord smsRecord = SmsRecord.builder()
                    .sendDate(Integer.valueOf(DateUtil.format(new Date(), DatePattern.PURE_DATE_PATTERN)))
                    .messageTemplateId(smsParam.getMessageTemplateId())
                    .phone(Long.valueOf(phone))
                    .supplierId(smsAccount.getSupplierId())
                    .supplierName(smsAccount.getSupplierName())
                    .msgContent(smsParam.getContent())
                    .seriesId(String.valueOf(System.currentTimeMillis()))
                    .chargingNum(1)
                    .status(SmsStatus.SEND_SUCCESS.getCode())
                    .reportContent("success")
                    .created(Math.toIntExact(DateUtil.currentSeconds()))
                    .updated(Math.toIntExact(DateUtil.currentSeconds()))
                    .build();

            smsRecordList.add(smsRecord);
        }
        return smsRecordList;
    }

    /**
     * 模拟网络抖动：平均耗时 50ms，标准差 15ms
     */
    public static void simulateNetworkJitter() throws InterruptedException {
        // nextGaussian() 产生均值为 0，标准差为 1 的正态分布随机数
        double gaussian = ThreadLocalRandom.current().nextGaussian();
        // 转换成均值 50，标准差 15 的耗时
        long latency = (long) (50 + (gaussian * 15));

        // 限制耗时在 10ms 到 3000ms（模拟极度偶尔的网络超时）之间
        latency = Math.clamp(latency, 10, 3000);


        TimeUnit.MILLISECONDS.sleep(latency);

    }
}
