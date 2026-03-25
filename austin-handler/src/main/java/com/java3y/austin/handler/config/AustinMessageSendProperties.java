package com.java3y.austin.handler.config;

import com.java3y.austin.common.enums.MessageType;
import com.java3y.austin.handler.deduplication.DeduplicationType;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Data
@Component
@RefreshScope
@ConfigurationProperties(prefix = "austin.message-send")
public class AustinMessageSendProperties {
    private List<Long> discardMsgIds;
    private Map<DeduplicationType, DeduplicationConfig> deduplicationRule;
    private List<Map<MessageType, List<SmsScriptConfig>>> msgTypeSmsConfig;
    private Map<String, Integer> flowControlRule;


    public record DeduplicationConfig(int num, long time) {
    }


    public record SmsScriptConfig(int weight, String scriptName) {
    }
}
