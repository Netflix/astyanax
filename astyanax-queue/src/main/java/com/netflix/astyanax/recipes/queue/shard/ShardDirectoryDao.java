package com.netflix.astyanax.recipes.queue.shard;

import java.util.Collection;

import com.netflix.astyanax.recipes.queue.exception.MessageQueueException;

/**
 * Dao for keeping track of shards
 * 
 * @author elandau
 */
public interface ShardDirectoryDao {
    /**
     * Create the underlying storage for the DAO
     */
    public void createStorage();
    
    /**
     * Register a new shard to be tracked.  The shard is available to read until it
     * is finialized
     * @param shardId
     * @param startTime     Time of first element in the shard
     */
    public void registerShard(String shardId, long startTime);

    /**
     * Discard a shard after it is no longer needed
     * @param shardId
     */
    public void discardShard(String shardId);
    
    /**
     * Return a list of all valid shards
     * @return
     * @throws MessageQueueException 
     */
    public Collection<String> listShards() throws MessageQueueException;

    /**
     * Finalize a shard to indicate that no more messages may be written to it.
     * @param shardName
     */
    void finalizeShard(String shardName);
}
