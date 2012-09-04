package com.netflix.astyanax.retry;

import com.netflix.astyanax.util.StringUtils;

/**
 * Bounded exponential backoff that will wait for no more than a provided max amount of time.
 *
 * The following examples show the maximum wait time for each attempt
 *  ExponentalBackoff(250, 10)
 *      250 500 1000 2000 4000 8000 16000 32000 64000 128000
 *
 *  BoundedExponentialBackoff(250, 5000, 10)
 *      250 500 1000 2000 4000 5000 5000 5000 5000 5000
 *
 * @author elandau
 *
 */
public class BoundedExponentialBackoff extends ExponentialBackoff {

    private final int maxSleepTimeMs;

    public BoundedExponentialBackoff(int baseSleepTimeMs, int maxSleepTimeMs, int max) {
        super(baseSleepTimeMs, max);
        this.maxSleepTimeMs = maxSleepTimeMs;
    }

    public long getSleepTimeMs() {
        return Math.max(maxSleepTimeMs, super.getSleepTimeMs());
    }

    @Override
    public RetryPolicy duplicate() {
        return new BoundedExponentialBackoff(this.getBaseSleepTimeMs(), this.maxSleepTimeMs, getMaxAttemptCount());
    }

    public String toString() {
        return StringUtils.joinClassAttributeValues(this, "BoundedExponentialBackoff", BoundedExponentialBackoff.class);
    }
}
