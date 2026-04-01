package com.java3y;

import cn.hutool.core.util.IdUtil;
import com.java3y.austin.AustinApplication;
import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.common.domain.TraceInfo;
import com.java3y.austin.common.enums.AnchorState;
import com.java3y.austin.service.api.impl.service.TraceServiceImpl;
import com.java3y.austin.support.utils.TraceUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Set;

@SpringBootTest(classes = AustinApplication.class)
public class TraceTest {
    @Autowired
    TraceUtils traceUtils;

    @Autowired
    TraceServiceImpl traceServiceImpl;


    @Test
    public void nacosTest() throws InterruptedException {
        TaskInfo taskInfo = TaskInfo.builder()
                .bizId(IdUtil.fastSimpleUUID())
                .traceId(IdUtil.fastSimpleUUID())
                .messageId(IdUtil.fastSimpleUUID())
                .receiver(Set.of("15638888888", "13682380232", "15934829923"))
                .messageTemplateId(23L)
                .build();

        traceUtils.trace(new TraceInfo(taskInfo, AnchorState.SEND_SUCCESS));

        Thread.sleep(10000);

        String traceId = taskInfo.getTraceId();
        traceServiceImpl.traceByTraceId(traceId).forEach(System.out::println);
    }
}

