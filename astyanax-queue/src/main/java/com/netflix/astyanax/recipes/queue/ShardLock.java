package com.netflix.astyanax.recipes.queue;

/**
 * Interface for a queue shard lock.
 *
 * @author pbhattacharyya
 */
public interface ShardLock {

    String getShardName();
}
