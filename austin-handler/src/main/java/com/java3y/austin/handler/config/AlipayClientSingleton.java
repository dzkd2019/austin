package com.java3y.austin.handler.config;

import com.alipay.api.AlipayApiException;
import com.alipay.api.AlipayConfig;
import com.alipay.api.DefaultAlipayClient;
import com.java3y.austin.common.constant.SendChanelUrlConstant;
import com.java3y.austin.common.dto.account.AlipayMiniProgramAccount;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 初始化支付宝小程序 单例
 *
 * @author 丁新东
 * @date 2022-12-07
 */
public class AlipayClientSingleton {


    private static final Map<String, DefaultAlipayClient> ALIPAY_CLIENT_MAP = new ConcurrentHashMap<>();

    private AlipayClientSingleton() {
    }

    public static DefaultAlipayClient getSingleton(AlipayMiniProgramAccount alipayMiniProgramAccount) throws AlipayApiException {
        return ALIPAY_CLIENT_MAP.computeIfAbsent(alipayMiniProgramAccount.getAppId(), appId -> {
            AlipayConfig alipayConfig = getAlipayConfig(alipayMiniProgramAccount);
            try {
                return new DefaultAlipayClient(alipayConfig);
            } catch (AlipayApiException e) {
                throw new RuntimeException("创建 AlipayClient 失败，AppId: " + appId, e);
            }
        });
    }

    private static AlipayConfig getAlipayConfig(AlipayMiniProgramAccount alipayMiniProgramAccount) {
        AlipayConfig alipayConfig = new AlipayConfig();
        alipayConfig.setServerUrl(SendChanelUrlConstant.ALI_MINI_PROGRAM_GATEWAY_URL);
        alipayConfig.setAppId(alipayMiniProgramAccount.getAppId());
        alipayConfig.setPrivateKey(alipayMiniProgramAccount.getPrivateKey());
        alipayConfig.setFormat("json");
        alipayConfig.setAlipayPublicKey(alipayMiniProgramAccount.getAlipayPublicKey());
        alipayConfig.setCharset("utf-8");
        alipayConfig.setSignType("RSA2");
        return alipayConfig;
    }
}
