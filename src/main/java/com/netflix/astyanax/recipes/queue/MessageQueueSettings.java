package com.netflix.astyanax.recipes.queue;

import java.util.concurrent.TimeUnit;

/**
 * MessageQueueSettings settings that are persisted to cassandra
 */
class MessageQueueSettings {
    public static final Integer       DEFAULT_RETENTION_TIMEOUT = null;
    public static final int           DEFAULT_SHARD_COUNT       = 1;
    public static final long          DEFAULT_BUCKET_DURATION   = TimeUnit.MICROSECONDS.convert(30,  TimeUnit.SECONDS);
    public static final int           DEFAULT_BUCKET_COUNT      = 1;

    private long      partitionDuration = DEFAULT_BUCKET_DURATION;
    private int       partitionCount    = DEFAULT_BUCKET_COUNT;
    private Integer   retentionTimeout  = DEFAULT_RETENTION_TIMEOUT;
    private int       shardCount        = DEFAULT_SHARD_COUNT;
    
    public long getPartitionDuration() {
        return partitionDuration;
    }
    public int getPartitionCount() {
        return partitionCount;
    }
    public Integer getRetentionTimeout() {
        return retentionTimeout;
    }
    public int getShardCount() {
        return shardCount;
    }
    public void setPartitionDuration(long partitionDuration) {
        this.partitionDuration = partitionDuration;
    }
    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }
    public void setRetentionTimeout(Integer retentionTimeout) {
        this.retentionTimeout = retentionTimeout;
    }
    public void setRetentionTimeout(Long retentionTimeout, TimeUnit units) {
        this.retentionTimeout = (int)TimeUnit.SECONDS.convert(retentionTimeout, units);
    }
    public void setShardCount(int shardCount) {
        this.shardCount = shardCount;
    }
    
    @Override
    public String toString() {
        return "MessageQueueSettings [partitionDuration=" + partitionDuration + ", partitionCount=" + partitionCount
                + ", visibilityTimeout=" + retentionTimeout + ", shardCount=" + shardCount + "]";
    }
}