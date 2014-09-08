package com.netflix.astyanax.recipes.queue;

import java.util.concurrent.TimeUnit;

/**
 * MessageQueueSettings settings that are persisted to cassandra
 */
public class MessageQueueMetadata  {
    public static final String        DEFAULT_QUEUE_NAME        = "Queue";
    public static final Integer       DEFAULT_RETENTION_TIMEOUT = null;
    public static final int           DEFAULT_SHARD_COUNT       = 1;
    public static final Long          DEFAULT_BUCKET_DURATION   = null; 
    public static final int           DEFAULT_BUCKET_COUNT      = 1;
    public static final Integer       DEFAULT_HISTORY_TTL       = null;
    
    @Deprecated
    public static final long          DEFAULT_POLL_WAIT         = TimeUnit.MILLISECONDS.convert(100, TimeUnit.MILLISECONDS);
    
    private Long      partitionDuration = DEFAULT_BUCKET_DURATION;
    private int       partitionCount    = DEFAULT_BUCKET_COUNT;
    private Integer   retentionTimeout  = DEFAULT_RETENTION_TIMEOUT;
    private int       shardCount        = DEFAULT_SHARD_COUNT;
    private Integer   historyTtl        = DEFAULT_HISTORY_TTL;
    private String    queueName         = DEFAULT_QUEUE_NAME;
    
    @Deprecated
    private long      pollInterval      = DEFAULT_POLL_WAIT;
    
    public Long getPartitionDuration() {
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
    public Integer getHistoryTtl() {
        return historyTtl;
    }
    public void setHistoryTtl(Integer historyTtl) {
        this.historyTtl = historyTtl;
    }
    public String getQueueName() {
        return queueName;
    }
    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }
    
    /**
     * Define this on the ShardReaderPolicy
     * @return
     */
    @Deprecated
    public long getPollInterval() {
        return pollInterval;
    }
    /**
     * Define this on the ShardReaderPolicy
     * @return
     */
    @Deprecated
    public void setPollInterval(long pollInterval) {
        this.pollInterval = pollInterval;
    }
    
    @Override
    public String toString() {
        return "MessageQueueSettings [partitionDuration=" + partitionDuration + ", partitionCount=" + partitionCount
                + ", retentionTimeout=" + retentionTimeout + ", shardCount=" + shardCount + ", historyTtl=" + historyTtl
                + ", queueName=" + queueName + ", pollInterval=" + pollInterval + "]";
    }
}
