package com.java3y.austin.handler.backpressure.config;

import com.java3y.austin.handler.backpressure.WaterMarkConfig;
import lombok.Data;
import org.springframework.stereotype.Component;

import java.util.Map;

@Data
public class BackPressureProperties {

    private WaterMarkConfig defaultWaterMarkConfig;

    private Map<String, WaterMarkConfig> channelConfigs;

}
