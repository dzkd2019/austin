package com.java3y.austin.handler.backpressure;

public record WaterMarkConfig(int highWaterMark, int lowWaterMark) {
    public WaterMarkConfig {
        if (lowWaterMark <= 0) {
            throw new IllegalArgumentException("低水位必须大于 0");
        }
        if (highWaterMark <= lowWaterMark) {
            throw new IllegalArgumentException(String.format("高水位(%d)必须严格大于低水位(%d)", highWaterMark, lowWaterMark));
        }
    }
}
