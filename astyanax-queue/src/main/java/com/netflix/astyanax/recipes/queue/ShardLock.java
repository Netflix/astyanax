package com.netflix.astyanax.recipes.queue;

/**
 * Interface for a queue shard lock.
 *
 * @author pbhattacharyya
 */
public interface ShardLock {

    /**
     * Name of shard being locked.
     * @return
     */
    String getShardName();
    
    /**
     * For implementations of the shard lock are tied to the underlying storage
     * for messages and may affect the message
     * @return
     */
    int getExtraMessagesToRead();
}
