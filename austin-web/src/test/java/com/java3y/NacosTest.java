package com.java3y;

import com.java3y.austin.AustinApplication;
import com.java3y.austin.handler.config.AustinMessageSendProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = AustinApplication.class)
public class NacosTest {

    @Autowired
    AustinMessageSendProperties properties;


    @Value("${austin.cron.pending.permits}")
    int permits;

    @Test
    public void test() {
        System.out.println(properties);
//        System.out.println(backPressureProperties);
        System.out.println(permits);
    }
}
