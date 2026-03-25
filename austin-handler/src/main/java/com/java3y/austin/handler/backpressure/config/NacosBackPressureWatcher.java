package com.java3y.austin.handler.backpressure.config;

import com.alibaba.cloud.nacos.annotation.NacosConfigListener;
import com.alibaba.fastjson2.JSON;
import com.java3y.austin.handler.backpressure.VirtualThreadBackPressureManager;
import com.java3y.austin.handler.backpressure.WaterMarkConfig;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

@Slf4j
@Component
public class NacosBackPressureWatcher {
    private final VirtualThreadBackPressureManager backPressureManager;

    public NacosBackPressureWatcher(VirtualThreadBackPressureManager backPressureManager) {
        this.backPressureManager = backPressureManager;
    }


    @NacosConfigListener(dataId = "austin-backpressure.json", group = "DEFAULT_GROUP", initNotify = true)
    public void onConfigChanged(String configInfo) {
        try {
            log.info("收到 Nacos 动态背压配置下发: \n{}", configInfo);

            BackPressureProperties properties = JSON.parseObject(configInfo, BackPressureProperties.class);
            if (properties == null) return;

            if (properties.getDefaultWaterMarkConfig() != null) {
                backPressureManager.updateDefaultWaterMarks(
                        properties.getDefaultWaterMarkConfig().highWaterMark(),
                        properties.getDefaultWaterMarkConfig().lowWaterMark()
                );
            }

            handleChannelConfigsDiff(properties.getChannelConfigs());
        } catch (Exception e) {
            log.error("解析并应用 Nacos 背压配置失败，将维持当前运行状态。错误配置: {}", configInfo, e);
        }
    }

    private void handleChannelConfigsDiff(Map<String, WaterMarkConfig> configs) {
        Set<String> currentCustomizedGroups = backPressureManager.getCustomizedGroupIds();

        if (configs != null && !configs.isEmpty()) {
            configs.forEach((groupId, config) -> {
                backPressureManager.setWaterMark(groupId, config);
                currentCustomizedGroups.remove(groupId);
            });
        }

        for (String groupId : currentCustomizedGroups) {
            log.info("Nacos移除了渠道 [{}] 的独立配置，恢复为默认水位线", groupId);
            backPressureManager.resetToDefaultWaterMark(groupId);
        }
    }
}
