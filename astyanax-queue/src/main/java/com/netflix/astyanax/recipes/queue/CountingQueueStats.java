/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.astyanax.recipes.queue;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CountingQueueStats implements MessageQueueStats {
    private AtomicLong emptyPartitionCount = new AtomicLong();
    private AtomicLong lockContentionCount = new AtomicLong();
    private AtomicLong eventProcessCount   = new AtomicLong();
    private AtomicLong eventReprocessCount = new AtomicLong();
    private AtomicLong expiredLockCount    = new AtomicLong();
    private AtomicLong ackMessageCount     = new AtomicLong();
    private AtomicLong sendMessageCount    = new AtomicLong();
    private AtomicLong invalidTaskCount    = new AtomicLong();
    private AtomicLong persistErrorCount   = new AtomicLong();
    
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
    public void incAckMessageCount() {
        ackMessageCount.incrementAndGet();
    }

    @Override
    public void incInvalidMessageCount() {
        invalidTaskCount.incrementAndGet();
    }

    @Override
    public void incPersistError() {
        persistErrorCount.incrementAndGet();
    }

    @Override
    public long getEmptyPartitionCount() {
        return this.emptyPartitionCount.get();
    }

    @Override
    public long getLockCountentionCount() {
        return this.lockContentionCount.get();
    }

    @Override
    public long getProcessCount() {
        return this.eventProcessCount.get();
    }

    @Override
    public long getReprocessCount() {
        return this.eventReprocessCount.get();
    }

    @Override
    public long getExpiredLockCount() {
        return this.expiredLockCount.get();
    }

    @Override
    public long getAckMessageCount() {
        return this.ackMessageCount.get();
    }

    @Override
    public long getSendMessageCount() {
        return this.sendMessageCount.get();
    }

    @Override
    public long getInvalidMessageCount() {
        return this.invalidTaskCount.get();
    }

    @Override
    public long getPersistErrorCount() {
        return this.persistErrorCount.get();
    }
    
    @Override
    public String toString() {
        return "CountingQueueStats [empty=" + emptyPartitionCount.get() 
                + ", cont="     + lockContentionCount.get()
                + ", ok="       + eventProcessCount .get()
                + ", redo="     + eventReprocessCount.get()
                + ", exp="      + expiredLockCount .get()
                + ", released=" + ackMessageCount .get()
                + ", new="      + sendMessageCount .get()
                + ", invalid="  + invalidTaskCount + "]";
    }
}
