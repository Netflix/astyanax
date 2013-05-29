package com.netflix.astyanax.thrift;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.netflix.astyanax.recipes.locks.BusyLockException;
import com.netflix.astyanax.recipes.queue.ShardLock;
import com.netflix.astyanax.recipes.queue.ShardLockManager;

/**
 * A shard lock manager implementation.
 */
public class ReentrantLockManager implements ShardLockManager {

    /**
     * A shard lock implementation that uses a ReentrantLock.
     */
    static class ReentrantShardLock implements ShardLock {

        private ReentrantLock lock;
        private String shardName;

        public ReentrantShardLock(ReentrantLock lock, String shardName) {
            this.lock = lock;
            this.shardName = shardName;
        }

        @Override
        public String getShardName() {
            return shardName;
        }

        public ReentrantLock getLock() {
            return lock;
        }
    }
    
    private ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<String, ReentrantLock>();
    private ConcurrentHashMap<String, AtomicInteger> busyLockCounts = new ConcurrentHashMap<String, AtomicInteger>();
    private AtomicLong lockAttempts = new AtomicLong();

    @Override
    public ShardLock acquireLock(String shardName) throws BusyLockException {
        locks.putIfAbsent(shardName, new ReentrantLock());
        ReentrantLock l = locks.get(shardName);
        try {
            lockAttempts.incrementAndGet();
            if (l.tryLock()) {
                return new ReentrantShardLock(l, shardName);
            } else {
                busyLockCounts.putIfAbsent(shardName, new AtomicInteger());
                busyLockCounts.get(shardName).incrementAndGet();
                throw new BusyLockException("Shard " + shardName + " is already locked" + ": busy lock count " + busyLockCounts.get(shardName));
            }
        } catch (Exception e) {
            throw new BusyLockException("Could not lock shard " + shardName, e);
        }
    }

    @Override
    public void releaseLock(ShardLock lock) {
        if(lock!=null) {
            ReentrantShardLock rsl = (ReentrantShardLock) lock;
            rsl.getLock().unlock();
        }
    }

    public Map<String,AtomicInteger> getBusyLockCounts() {
        return busyLockCounts;
    }

    public long getLockAttempts() {
        return lockAttempts.longValue();
    }
}
