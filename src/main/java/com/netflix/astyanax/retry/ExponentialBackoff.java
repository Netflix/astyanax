package com.netflix.astyanax.retry;

import java.util.Random;

import com.netflix.astyanax.util.StringUtils;

public class ExponentialBackoff extends SleepingRetryPolicy {

    private final Random random = new Random();
    private final int baseSleepTimeMs;

    public ExponentialBackoff(int baseSleepTimeMs, int max) {
        super(max);
        this.baseSleepTimeMs = baseSleepTimeMs;
    }

    @Override
    public long getSleepTimeMs() {
        return baseSleepTimeMs
                * Math.max(1, random.nextInt(1 << (this.getAttemptCount() + 1)));
    }

    @Override
    public RetryPolicy duplicate() {
        return new ExponentialBackoff(baseSleepTimeMs, getMax());
    }

    public String toString() {
        return StringUtils.joinClassAttributeValues(this, "ExponentialBackoff",
                ExponentialBackoff.class);
    }
}
