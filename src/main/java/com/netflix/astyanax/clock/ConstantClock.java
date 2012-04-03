package com.netflix.astyanax.clock;

import com.netflix.astyanax.Clock;

public class ConstantClock implements Clock {

    private final long timestamp;

    public ConstantClock(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public long getCurrentTime() {
        return timestamp;
    }
}
