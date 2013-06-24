package com.netflix.astyanax.recipes.queue;

import com.netflix.astyanax.recipes.locks.BusyLockException;
import com.netflix.astyanax.recipes.queue.exception.MessageQueueException;

/**
 * Interface for a queue shard lock manager.
 *
 * @author pbhattacharyya
 */
public interface ShardLockManager {

    ShardLock acquireLock(String shardName) throws BusyLockException, MessageQueueException;

    void releaseLock(ShardLock lock) throws MessageQueueException;
}
