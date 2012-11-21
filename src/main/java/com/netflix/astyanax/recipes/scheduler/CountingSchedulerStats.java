package com.netflix.astyanax.recipes.scheduler;

import java.util.concurrent.atomic.AtomicLong;

public class CountingSchedulerStats implements SchedulerStats {
    private AtomicLong emptyPartitionCount = new AtomicLong();
    private AtomicLong lockContentionCount = new AtomicLong();
    private AtomicLong eventProcessCount   = new AtomicLong();
    private AtomicLong eventReprocessCount = new AtomicLong();
    private AtomicLong expiredLockCount    = new AtomicLong();
    private AtomicLong releaseTaskCount    = new AtomicLong();
    private AtomicLong submitTaskCount     = new AtomicLong();
    private AtomicLong invalidTaskCount    = new AtomicLong();
    
    @Override
    public void incEmptyPartitionCount() {
        emptyPartitionCount.incrementAndGet();
    }

    @Override
    public void incLockContentionCount() {
        lockContentionCount.incrementAndGet();
    }

    @Override
    public void incProcessCount() {
        eventProcessCount.incrementAndGet();
    }

    @Override
    public void incReprocessCount() {
        eventReprocessCount.incrementAndGet();
    }

    @Override
    public void incExpiredLockCount() {
        expiredLockCount.incrementAndGet();        
    }

    @Override
    public void incSubmitTaskCount() {
        submitTaskCount.incrementAndGet();
    }

    @Override
    public void incFinishTaskCount() {
        releaseTaskCount.incrementAndGet();
    }

    @Override
    public void incInvalidTaskCount() {
        invalidTaskCount.incrementAndGet();
    }

    @Override
    public String toString() {
        return "CountingSchedulerStats [empty=" + emptyPartitionCount.get() 
                + ", cont="     + lockContentionCount.get()
                + ", ok="       + eventProcessCount .get()
                + ", redo="     + eventReprocessCount.get()
                + ", exp="      + expiredLockCount .get()
                + ", rel="      + releaseTaskCount .get()
                + ", new="      + submitTaskCount .get()
                + ", invalid="  + invalidTaskCount + "]";
    }
}
