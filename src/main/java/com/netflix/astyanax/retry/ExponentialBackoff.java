package com.netflix.astyanax.retry;

import java.util.Random;

import com.netflix.astyanax.util.StringUtils;

/**
 * Unbounded exponential backoff will sleep a random number of intervals within an
 * exponentially increasing number of intervals.
 *
 * @author elandau
 *
 */
public class ExponentialBackoff extends SleepingRetryPolicy {

    private final Random random = new Random();
    private final int baseSleepTimeMs;

    public ExponentialBackoff(int baseSleepTimeMs, int maxAttempts) {
        super(maxAttempts);
        this.baseSleepTimeMs = baseSleepTimeMs;
    }

    @Override
    public long getSleepTimeMs() {
        return baseSleepTimeMs * Math.max(1, random.nextInt(1 << (this.getAttemptCount() + 1)));
    }

    public int getBaseSleepTimeMs() {
        return baseSleepTimeMs;
    }

    @Override
    public RetryPolicy duplicate() {
        return new ExponentialBackoff(baseSleepTimeMs, getMaxAttemptCount());
    }

    public String toString() {
        return StringUtils.joinClassAttributeValues(this, "ExponentialBackoff", ExponentialBackoff.class);
    }
}
