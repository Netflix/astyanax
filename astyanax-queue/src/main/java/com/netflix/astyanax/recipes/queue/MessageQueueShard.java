package com.netflix.astyanax.recipes.queue;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Track the state of a partition
 * 
 * @author elandau
 */
public class MessageQueueShard implements MessageQueueShardStats {
    private volatile int   lastCount = 0;
    private final String   name;
    private final int      partition;
    private final int      shard;
    private final AtomicLong readCount = new AtomicLong();
    private final AtomicLong writeCount = new AtomicLong();
    
    public MessageQueueShard(String name, int partition, int shard) {
        this.name      = name;
        this.partition = partition;
        this.shard     = shard;
    }
    
    public String getName() {
        return name;
    }
    
    public void setLastCount(int count) {
        this.lastCount = count;
        this.readCount.addAndGet(count);
    }
    
    @Override
    public long getReadCount() {
        return this.readCount.get();
    }
    
    @Override
    public long getWriteCount() {
        return this.writeCount.get();
    }
    
    @Override
    public long getLastReadCount() {
        return this.lastCount;
    }
    
    public void incInsertCount(int count) {
        this.writeCount.addAndGet(count);
    }
    
    public int getShard() {
        return this.shard;
    }
    
    public int getPartition() {
        return this.partition;
    }

    @Override
    public String toString() {
        return "Partition [lastCount=" + lastCount + ", name=" + name + ", partition=" + partition + ", shard=" + shard + "]";
    }
}