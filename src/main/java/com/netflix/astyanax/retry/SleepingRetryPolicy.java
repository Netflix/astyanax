package com.netflix.astyanax.retry;

import com.netflix.astyanax.util.StringUtils;

/**
 * Base sleeping retry policy with optional count limit
 * 
 * @author elandau
 * 
 */
public abstract class SleepingRetryPolicy implements RetryPolicy {
    private final int max;
    private int attempts;

    public SleepingRetryPolicy(int max) {
        this.max = max;
        this.attempts = 0;
    }

    public boolean allowRetry() {
        if (max == -1 || attempts < max) {
            try {
                Thread.sleep(getSleepTimeMs());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            attempts++;
            return true;
        }
        return false;
    }

    public abstract long getSleepTimeMs();

    @Override
    public void begin() {
        this.attempts = 0;
    }

    @Override
    public void success() {
    }

    @Override
    public void failure(Exception e) {
    }

    public int getAttemptCount() {
        return attempts;
    }

    public int getMax() {
        return max;
    }

    public String toString() {
        return StringUtils.joinClassAttributeValues(this, "SleepingRetryPolicy", SleepingRetryPolicy.class);
    }
}
