package com.netflix.astyanax.recipes.queue.shard;

import java.util.Collection;
import java.util.Map;

import com.netflix.astyanax.recipes.queue.MessageQueueMetadata;
import com.netflix.astyanax.recipes.queue.MessageQueueShard;
import com.netflix.astyanax.recipes.queue.MessageQueueShardStats;

/**
 * Policy for scheduling shards to be processed
 * 
 * @author elandau
 *
 */
public interface ShardReaderPolicy {
    public static interface Factory {
        public ShardReaderPolicy create(MessageQueueMetadata metadata);
    }
    
    /**
     * Acquire the next shard to be processed.  Must call releaseShard when done reading
     * from the shard
     * @return A reference to the acquired shard.  
     * @throws InterruptedException 
     */
    public MessageQueueShard nextShard() throws InterruptedException;
    
    /**
     * Release a shard after acquiring and reading messages
     * @param shard
     */
    public void releaseShard(MessageQueueShard shard, int messagesRead);
    
    /**
     * @return List all the shards
     */
    public Collection<MessageQueueShard> listShards();
    
    /**
     * @return Return map of all shard stats
     */
    public Map<String, MessageQueueShardStats> getShardStats();
    
    /**
     * @return number of shards in the work or active queue
     */
    public int getWorkQueueDepth();

    /**
     * @return number of shards in the idle queue
     */
    public int getIdleQueueDepth();

    /**
     * a ShardReaderPolicy is in catch up mode when more than two time buckets are in the work or active queue.
     * @return is the shard reader catching up.
     */
    public boolean isCatchingUp();

    /**
     * @return the correct polling interval.  if in catch up mode returns the catchUpPollInterval otherwise
     * returns the "normal" pollInterval
     */
    public long getPollInterval();
}
