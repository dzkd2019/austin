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

public class MockAssembleUtils {
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
}
