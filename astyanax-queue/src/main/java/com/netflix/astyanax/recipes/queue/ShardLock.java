package com.netflix.astyanax.recipes.queue;

/**
 * Represents a shard lock.
 *
 * @author pbhattacharyya
 */
public interface ShardLock {
    
    String getShardName();
}
