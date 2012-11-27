package com.netflix.astyanax.recipes.scheduler;

/**
 * Track the state of a partition
 * 
 * @author elandau
 */
public class SchedulerPartition {
    private volatile int   lastCount = 1;
    private final String   name;
    private final int      partition;
    private final int      shard;
    
    public SchedulerPartition(String name, int partition, int shard) {
        this.name      = name;
        this.partition = partition;
        this.shard     = shard;
    }
    
    public String getName() {
        return name;
    }
    
    public void setLastCount(int count) {
        this.lastCount = count;
    }
    
    public int getLastCount() {
        return this.lastCount;
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