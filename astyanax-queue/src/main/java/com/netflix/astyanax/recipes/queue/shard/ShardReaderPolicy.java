package com.netflix.astyanax.recipes.queue.shard;

import java.util.Collection;
import java.util.Map;

import com.netflix.astyanax.recipes.queue.MessageQueueShard;
import com.netflix.astyanax.recipes.queue.MessageQueueShardStats;

/**
 * Policy for scheduling shards to be processed
 * 
 * @author elandau
 *
 */
public interface ShardReaderPolicy {
    /**
     * Acquire the next shard to be processed.  Must call releaseShard when done reading
     * from the shard
     * @return
     * @throws InterruptedException 
     */
    MessageQueueShard nextShard() throws InterruptedException;
    
    /**
     * Release a shard after acquiring and reading messages
     * @param shard
     */
    void releaseShard(MessageQueueShard shard, int messagesRead);
    
    /**
     * List all the shards
     * @return
     */
    Collection<MessageQueueShard> listShards();
    
    /**
     * Return map of all shard stats
     */
    Map<String, MessageQueueShardStats> getShardStats();
}
