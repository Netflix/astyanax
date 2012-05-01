package com.netflix.astyanax.retry;

import com.netflix.astyanax.util.StringUtils;

public class ConstantBackoff extends SleepingRetryPolicy {

    private final int sleepTimeMs;

    public ConstantBackoff(int sleepTimeMs, int max) {
        super(max);
        this.sleepTimeMs = sleepTimeMs;
    }

    @Override
    public long getSleepTimeMs() {
        return sleepTimeMs;
    }

    @Override
    public RetryPolicy duplicate() {
        return new ConstantBackoff(sleepTimeMs, getMax());
    }

    public String toString() {
        return StringUtils.joinClassAttributeValues(this, "ConstantBackoff", ConstantBackoff.class);
    }

}
