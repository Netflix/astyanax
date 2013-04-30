package com.netflix.astyanax.retry;

public class IndefiniteRetry implements RetryPolicy {

    private int counter = 1;
    
    @Override
    public void begin() {
        counter = 1;
    }

    @Override
    public void success() {
    }

    @Override
    public void failure(Exception e) {
        counter++;
    }

    @Override
    public boolean allowRetry() {
        return true;
    }

    @Override
    public int getAttemptCount() {
        return counter;
    }

    @Override
    public RetryPolicy duplicate() {
        return new IndefiniteRetry();
    }

}
