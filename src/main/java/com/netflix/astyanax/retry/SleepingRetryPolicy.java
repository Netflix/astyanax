package com.netflix.astyanax.retry;

import com.netflix.astyanax.util.StringUtils;

/**
 * Base sleeping retry policy with optional count limit.  The sleep time
 * is delegated to the subclass.
 * 
 * @author elandau
 * 
 */
public abstract class SleepingRetryPolicy implements RetryPolicy {
    private final int maxAttempts;
    private int attempts;

    public SleepingRetryPolicy(int max) {
        this.maxAttempts = max;
        this.attempts = 0;
    }

    public boolean allowRetry() {
        if (maxAttempts == -1 || attempts < maxAttempts) {
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

    @Deprecated
    public int getMax() {
        return getMaxAttemptCount();
    }
    
    public int getMaxAttemptCount() {
        return maxAttempts;
    }

    public String toString() {
        return StringUtils.joinClassAttributeValues(this, "SleepingRetryPolicy", SleepingRetryPolicy.class);
    }
}
