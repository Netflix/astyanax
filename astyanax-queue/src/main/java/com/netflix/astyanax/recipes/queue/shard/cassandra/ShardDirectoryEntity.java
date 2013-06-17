package com.netflix.astyanax.recipes.queue.shard.cassandra;

import javax.persistence.Column;
import javax.persistence.Id;

public class ShardDirectoryEntity {
    @Id
    private String queueName;
    
    @Column
    private String shardName;

    @Column
    private long   startTime;

    public ShardDirectoryEntity(String queueName, String shardName, long startTime) {
        super();
        this.queueName = queueName;
        this.shardName = shardName;
        this.startTime = startTime;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public String getShardName() {
        return shardName;
    }

    public void setShardName(String shardName) {
        this.shardName = shardName;
    }

    @Override
    public String toString() {
        return "ShardDirectoryEntity [queueName=" + queueName + ", shardName="
                + shardName + ", startTime=" + startTime + "]";
    }

    
}