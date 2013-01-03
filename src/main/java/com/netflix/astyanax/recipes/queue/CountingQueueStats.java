package com.netflix.astyanax.recipes.queue;

import java.util.concurrent.atomic.AtomicLong;

public class CountingQueueStats implements MessageQueueStats {
    private AtomicLong emptyPartitionCount = new AtomicLong();
    private AtomicLong lockContentionCount = new AtomicLong();
    private AtomicLong eventProcessCount   = new AtomicLong();
    private AtomicLong eventReprocessCount = new AtomicLong();
    private AtomicLong expiredLockCount    = new AtomicLong();
    private AtomicLong releaseMessageCount = new AtomicLong();
    private AtomicLong sendMessageCount    = new AtomicLong();
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
    public void incSendMessageCount() {
        sendMessageCount.incrementAndGet();
    }

    @Override
    public void incFinishMessageCount() {
        releaseMessageCount.incrementAndGet();
    }

    @Override
    public void incInvalidMessageCount() {
        invalidTaskCount.incrementAndGet();
    }

    @Override
    public String toString() {
        return "CountingQueueStats [empty=" + emptyPartitionCount.get() 
                + ", cont="     + lockContentionCount.get()
                + ", ok="       + eventProcessCount .get()
                + ", redo="     + eventReprocessCount.get()
                + ", exp="      + expiredLockCount .get()
                + ", rel="      + releaseMessageCount .get()
                + ", new="      + sendMessageCount .get()
                + ", invalid="  + invalidTaskCount + "]";
    }
}
