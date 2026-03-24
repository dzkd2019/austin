package com.java3y.austin.handler.receiver.kafka;

import com.java3y.austin.handler.utils.GroupIdMappingUtils;
import jakarta.annotation.PostConstruct;
import org.jspecify.annotations.NonNull;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ReceiverContainer implements ApplicationContextAware {
    private ApplicationContext applicationContext;
    private static final List<String> GROUP_IDS = GroupIdMappingUtils.getAllGroupIds();

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @PostConstruct
    public void init() {
        for (String _ : GROUP_IDS) {
            applicationContext.getBean(Receiver.class);
        }
    }
}
