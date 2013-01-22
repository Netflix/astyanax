package com.netflix.astyanax.recipes.queue.shard;

import java.util.Collection;

import com.netflix.astyanax.recipes.queue.MessageQueueShard;

/**
 * Policy for scheduling shards to be processed
 * 
 * @author elandau
 *
 */
public interface ShardStrategy {
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
}
