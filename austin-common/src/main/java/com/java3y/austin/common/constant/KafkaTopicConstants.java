package com.java3y.austin.common.constant;

import java.util.List;

/**
 * Kafka 业务 Topic 常量（方案B：topic = groupId）
 */
public final class KafkaTopicConstants {

    private KafkaTopicConstants() {
    }

    public static final String IM_NOTICE = "im.notice";
    public static final String IM_MARKETING = "im.marketing";
    public static final String IM_AUTH_CODE = "im.auth_code";

    public static final String PUSH_NOTICE = "push.notice";
    public static final String PUSH_MARKETING = "push.marketing";
    public static final String PUSH_AUTH_CODE = "push.auth_code";

    public static final String SMS_NOTICE = "sms.notice";
    public static final String SMS_MARKETING = "sms.marketing";
    public static final String SMS_AUTH_CODE = "sms.auth_code";

    public static final String EMAIL_NOTICE = "email.notice";
    public static final String EMAIL_MARKETING = "email.marketing";
    public static final String EMAIL_AUTH_CODE = "email.auth_code";

    public static final String OFFICIAL_ACCOUNTS_NOTICE = "official_accounts.notice";
    public static final String OFFICIAL_ACCOUNTS_MARKETING = "official_accounts.marketing";
    public static final String OFFICIAL_ACCOUNTS_AUTH_CODE = "official_accounts.auth_code";

    public static final String MINI_PROGRAM_NOTICE = "mini_program.notice";
    public static final String MINI_PROGRAM_MARKETING = "mini_program.marketing";
    public static final String MINI_PROGRAM_AUTH_CODE = "mini_program.auth_code";

    public static final String ENTERPRISE_WE_CHAT_NOTICE = "enterprise_we_chat.notice";
    public static final String ENTERPRISE_WE_CHAT_MARKETING = "enterprise_we_chat.marketing";
    public static final String ENTERPRISE_WE_CHAT_AUTH_CODE = "enterprise_we_chat.auth_code";

    public static final String DING_DING_ROBOT_NOTICE = "ding_ding_robot.notice";
    public static final String DING_DING_ROBOT_MARKETING = "ding_ding_robot.marketing";
    public static final String DING_DING_ROBOT_AUTH_CODE = "ding_ding_robot.auth_code";

    public static final String DING_DING_WORK_NOTICE_NOTICE = "ding_ding_work_notice.notice";
    public static final String DING_DING_WORK_NOTICE_MARKETING = "ding_ding_work_notice.marketing";
    public static final String DING_DING_WORK_NOTICE_AUTH_CODE = "ding_ding_work_notice.auth_code";

    public static final String ENTERPRISE_WE_CHAT_ROBOT_NOTICE = "enterprise_we_chat_robot.notice";
    public static final String ENTERPRISE_WE_CHAT_ROBOT_MARKETING = "enterprise_we_chat_robot.marketing";
    public static final String ENTERPRISE_WE_CHAT_ROBOT_AUTH_CODE = "enterprise_we_chat_robot.auth_code";

    public static final String FEI_SHU_ROBOT_NOTICE = "fei_shu_robot.notice";
    public static final String FEI_SHU_ROBOT_MARKETING = "fei_shu_robot.marketing";
    public static final String FEI_SHU_ROBOT_AUTH_CODE = "fei_shu_robot.auth_code";

    public static final String ALIPAY_MINI_PROGRAM_NOTICE = "alipay_mini_program.notice";
    public static final String ALIPAY_MINI_PROGRAM_MARKETING = "alipay_mini_program.marketing";
    public static final String ALIPAY_MINI_PROGRAM_AUTH_CODE = "alipay_mini_program.auth_code";

    public static List<String> getAllBusinessTopics() {
        return List.of(
                IM_NOTICE, IM_MARKETING, IM_AUTH_CODE,
                PUSH_NOTICE, PUSH_MARKETING, PUSH_AUTH_CODE,
                SMS_NOTICE, SMS_MARKETING, SMS_AUTH_CODE,
                EMAIL_NOTICE, EMAIL_MARKETING, EMAIL_AUTH_CODE,
                OFFICIAL_ACCOUNTS_NOTICE, OFFICIAL_ACCOUNTS_MARKETING, OFFICIAL_ACCOUNTS_AUTH_CODE,
                MINI_PROGRAM_NOTICE, MINI_PROGRAM_MARKETING, MINI_PROGRAM_AUTH_CODE,
                ENTERPRISE_WE_CHAT_NOTICE, ENTERPRISE_WE_CHAT_MARKETING, ENTERPRISE_WE_CHAT_AUTH_CODE,
                DING_DING_ROBOT_NOTICE, DING_DING_ROBOT_MARKETING, DING_DING_ROBOT_AUTH_CODE,
                DING_DING_WORK_NOTICE_NOTICE, DING_DING_WORK_NOTICE_MARKETING, DING_DING_WORK_NOTICE_AUTH_CODE,
                ENTERPRISE_WE_CHAT_ROBOT_NOTICE, ENTERPRISE_WE_CHAT_ROBOT_MARKETING, ENTERPRISE_WE_CHAT_ROBOT_AUTH_CODE,
                FEI_SHU_ROBOT_NOTICE, FEI_SHU_ROBOT_MARKETING, FEI_SHU_ROBOT_AUTH_CODE,
                ALIPAY_MINI_PROGRAM_NOTICE, ALIPAY_MINI_PROGRAM_MARKETING, ALIPAY_MINI_PROGRAM_AUTH_CODE
        );
    }
}
