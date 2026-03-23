package com.java3y.austin.handler.backpressure;

import java.util.concurrent.atomic.LongAdder;

public record RtComputer(LongAdder callCount, LongAdder totalRtMs) {

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
