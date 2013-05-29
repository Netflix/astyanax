package com.netflix.astyanax.recipes.queue;

import java.util.concurrent.TimeUnit;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import com.google.common.base.Preconditions;

/**
 * Metadata for a specific message queue.  Clients are expected to read this metadata at startup
 * so that they may know how to access the queue.
 */
@Entity
public class MessageQueueInfo  {
    public static final String        DEFAULT_QUEUE_NAME        = "Queue";
    public static final int           DEFAULT_RETENTION_TIMEOUT = 0;
    public static final int           DEFAULT_SHARD_COUNT       = 1;
    public static final Long          DEFAULT_BUCKET_DURATION   = null; 
    public static final int           DEFAULT_BUCKET_COUNT      = 1;
    public static final Integer       DEFAULT_HISTORY_TTL       = null;
    public static final String        DEFAULT_COLUMN_FAMILY_NAME = null;
    
    public static class Builder {
        private String    queueName         = DEFAULT_QUEUE_NAME;
        private Long      partitionDuration = DEFAULT_BUCKET_DURATION;
        private int       partitionCount    = DEFAULT_BUCKET_COUNT;
        private int       retentionTimeout  = DEFAULT_RETENTION_TIMEOUT;
        private int       shardCount        = DEFAULT_SHARD_COUNT;
        private Integer   historyTtl        = DEFAULT_HISTORY_TTL;
        private String    columnFamilyBase  = DEFAULT_COLUMN_FAMILY_NAME;
        
        public Builder withQueueName(String queueName) {
            this.queueName = queueName;
            return this;
        }
        
        public Builder withColumnFamilyBase(String columnFamilyBaseName) {
            this.columnFamilyBase = columnFamilyBaseName;
            return this;
        }
        
        public Builder withShardCount(int count) {
            this.shardCount = count;
            return this;
        }

        public Builder withTimeBuckets(int bucketCount, int bucketDuration, TimeUnit units) {
            this.partitionDuration = TimeUnit.MICROSECONDS.convert(bucketDuration,  units);
            this.partitionCount    = bucketCount;
            return this;
        }
        
        public Builder withRetentionTimeout(Long retentionTimeout, TimeUnit units) {
            this.retentionTimeout = (int)TimeUnit.SECONDS.convert(retentionTimeout, units);
            return this;
        }
        
        public Builder withHistoryTtl(Integer historyTtl) {
            this.historyTtl = historyTtl;
            return this;
        }
        
        public MessageQueueInfo build() {
            Preconditions.checkNotNull(queueName, "Queue name cannot be null");
            return new MessageQueueInfo(this);
        }
        
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    @Deprecated
    public static final long          DEFAULT_POLL_WAIT         = TimeUnit.MILLISECONDS.convert(100, TimeUnit.MILLISECONDS);
    
    @Id
    private String    queueName         = DEFAULT_QUEUE_NAME;
    
    @Column(name="PARTITION_DURATION")
    private Long      partitionDuration = DEFAULT_BUCKET_DURATION;
    
    @Column(name="PARTITION_COUNT")
    private int       partitionCount    = DEFAULT_BUCKET_COUNT;
    
    @Column(name="RETENTION_TIMEOUT")
    private int       retentionTimeout  = DEFAULT_RETENTION_TIMEOUT;
    
    @Column(name="SHARD_COUNT")
    private int       shardCount        = DEFAULT_SHARD_COUNT;
    
    @Column(name="HISTORY_TTL")
    private Integer   historyTtl        = DEFAULT_HISTORY_TTL;
    
//    @Column(name="UNIQUE", unique=true)
//    private String    unique;
//    
    @Column(name="COLUMN_FAMILY")
    private String    columnFamilyBase = DEFAULT_COLUMN_FAMILY_NAME;
    
    public MessageQueueInfo() {
        
    }
    
    public MessageQueueInfo(Builder builder) {
        this.queueName          = builder.queueName;
        this.partitionCount     = builder.partitionCount;
        this.partitionDuration  = builder.partitionDuration;
        this.retentionTimeout   = builder.retentionTimeout;
        this.historyTtl         = builder.historyTtl;
        this.shardCount         = builder.shardCount;
        this.columnFamilyBase   = builder.columnFamilyBase;
    }
    
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
    public String getColumnFamilyBase() {
        return columnFamilyBase;
    }
    public void setColumnFamilyBase(String columnFamilyBase) {
        this.columnFamilyBase = columnFamilyBase;
    }

    @Override
    public String toString() {
        return "MessageQueueInfo ["
                + "queueName=" + queueName
                + ", columnFamilyBase=" + columnFamilyBase
                + ", partitionDuration=" + partitionDuration
                + ", partitionCount=" + partitionCount 
                + ", retentionTimeout=" + retentionTimeout 
                + ", shardCount=" + shardCount
                + ", historyTtl=" + historyTtl 
                + "]";
    }

}
