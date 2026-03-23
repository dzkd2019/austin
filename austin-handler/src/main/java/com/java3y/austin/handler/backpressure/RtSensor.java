package com.java3y.austin.handler.backpressure;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.LongAdder;

@Component
public class RtSensor {
    private final LongAdder callCount = new LongAdder();
    private final LongAdder totalRtMs = new LongAdder();

    public void record(long rtMs) {
        callCount.increment();
        totalRtMs.add(rtMs);
    }

    public long getAndResetAvgRtMs() {
        long count = callCount.sumThenReset();
        long total = totalRtMs.sumThenReset();
        return count == 0 ? 0 : total / count;
    }
}
