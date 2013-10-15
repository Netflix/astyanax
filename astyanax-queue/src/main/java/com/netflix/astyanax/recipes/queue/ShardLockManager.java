package com.netflix.astyanax.recipes.queue;

import com.netflix.astyanax.recipes.locks.BusyLockException;

/**
 * Interface for a queue shard lock manager.
 *
 * @author pbhattacharyya
 */
public interface ShardLockManager {

    ShardLock acquireLock(String shardName) throws BusyLockException;

    void releaseLock(ShardLock lock);
}
