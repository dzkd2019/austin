package com.java3y.austin.handler.handler;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class VirtualThreadBackPressureManager {
    private final AtomicInteger inFlightCount = new AtomicInteger(0);

    private final int highWaterMark;
    private final int lowWaterMark;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition drainCondition = lock.newCondition();

    public VirtualThreadBackPressureManager(int highWaterMark, int lowWaterMark) {
        if (lowWaterMark <= 0 || highWaterMark <= lowWaterMark) {
            throw new IllegalArgumentException("低水位必须严格小于高水位");
        }
        this.highWaterMark = highWaterMark;
        this.lowWaterMark = lowWaterMark;
    }

    public void increment() {
        inFlightCount.incrementAndGet();
    }

    public void decrement() {
        int current = inFlightCount.get();

        if (current < lowWaterMark) {
            lock.lock();
            try {
                drainCondition.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    public void awaitDrainIfNeeded() {
        if (inFlightCount.get() < highWaterMark) {
            return;
        }

        lock.lock();
        try {
            log.warn("触及高水位，({} / {})，停止分发，进入休眠等待水位下降到低水位", inFlightCount.get(), highWaterMark);

            while (inFlightCount.get() > lowWaterMark) {
                drainCondition.await();
            }
            log.info("水位已经回落至低水位 ({} / {}), 唤醒主线程，恢复分发", inFlightCount.get(), lowWaterMark);
        } catch (InterruptedException e) {
            log.error("等待水位回落时收到中断信号", e);
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
    }

    public int getCurrentInFlight() {
        return inFlightCount.get();
    }
}
