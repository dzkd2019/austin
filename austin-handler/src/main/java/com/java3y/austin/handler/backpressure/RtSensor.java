package com.java3y.austin.handler.backpressure;

import com.java3y.austin.handler.utils.GroupIdMappingUtils;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

@Component
public class RtSensor {
    private final Map<String, RtComputer> rtComputerMap = new ConcurrentHashMap<>();

    // 工厂函数：根据 groupId 创建一个新的 RtComputer 实例. 设置为static final变量，避免每次使用computeIfAbsent时都需要创建一个lambda内部类
    private static final Function<String, RtComputer> RT_COMPUTER_FACTORY = _ -> new RtComputer(new LongAdder(), new LongAdder());

    @PostConstruct
    public void init() {
        GroupIdMappingUtils.getAllGroupIds()
                .forEach(groupId -> {
                    rtComputerMap.put(groupId, RT_COMPUTER_FACTORY.apply(groupId));
                });
    }

    public void record(String groupId, long rtMs) {
        rtComputerMap.computeIfAbsent(groupId, RT_COMPUTER_FACTORY)
                .record(rtMs);
    }

    public long getAndResetAvgRtMs(String groupId) {
        RtComputer computer = rtComputerMap.get(groupId);
        if (computer == null) {
            return 0;
        }
        return computer.getAndResetAvgRtMs();
    }
}
