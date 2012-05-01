package com.netflix.astyanax.retry;

import com.netflix.astyanax.util.StringUtils;

public class RunOnce implements RetryPolicy {
    public static RunOnce instance = new RunOnce();

    public static RunOnce get() {
        return instance;
    }

    @Override
    public void begin() {
    }

    @Override
    public void success() {
    }

    @Override
    public void failure(Exception e) {
    }

    @Override
    public boolean allowRetry() {
        return false;
    }

    @Override
    public int getAttemptCount() {
        return 1;
    }

    @Override
    public RetryPolicy duplicate() {
        return RunOnce.get();
    }

    public String toString() {
        return StringUtils.joinClassAttributeValues(this, "RunOnce", RunOnce.class);
    }

}
