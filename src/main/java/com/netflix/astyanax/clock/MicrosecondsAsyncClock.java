package com.netflix.astyanax.clock;

import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.astyanax.Clock;

public class MicrosecondsAsyncClock implements Clock {

    private static final long serialVersionUID = -4671061000963496156L;
    private static final long ONE_THOUSAND = 1000L;

    private static AtomicInteger counter = new AtomicInteger(0);

    public MicrosecondsAsyncClock() {

    }

    @Override
    public long getCurrentTime() {
        // The following simulates a microseconds resolution by advancing a
        // static counter
        // every time a client calls the createClock method, simulating a tick.
        long us = System.currentTimeMillis() * ONE_THOUSAND;
        return us + counter.getAndIncrement() % ONE_THOUSAND;
    }

    public String toString() {
        return "MicrosecondsAsyncClock";
    }
}
