package com.netflix.astyanax.recipes.queue.shard;

import com.netflix.astyanax.recipes.queue.Message;

/**
 * Policy for getting a shard name based on the message.  Use this when your
 * sharding actually depends on information in the message such as the 
 * message key or timestamp.  This interface is used when writing the message
 * and is paired with a symetric ShardReaderPolicy.
 * 
 * @author elandau
 *
 */
public interface QueueShardPolicy {

    /**
     * @param message
     * @return Return the full shard name for the message.
     */
    public String getShardKey(Message message);

    /**
     * Notification from the queue shard reader that the last messages of a finalized
     * shard was read 
     * 
     * @param shardName
     */
    public void discardShard(String shardName);
}
