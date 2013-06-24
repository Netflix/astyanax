package com.netflix.astyanax.recipes.queue.zookeeper;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.recipes.locks.BusyLockException;
import com.netflix.astyanax.recipes.queue.ShardLock;
import com.netflix.astyanax.recipes.queue.ShardLockManager;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;

/**
 * Implementation of a queue shard lock using ZooKeeper (via. Curator)
 * 
 * @author pbhattacharyya
 *
 */
public class ZkShardLockManager implements ShardLockManager {

    private static final Logger LOG = LoggerFactory.getLogger(ZkShardLockManager.class);
    
    private static final String PATH_PREFIX            = "/shardlock/";
    private static final int    MAX_MUTEX_WAIT_TIME_MS = 15;
    
    private final CuratorFramework curator;
    
    public ZkShardLockManager(CuratorFramework curator)  {
        this.curator = curator;
    }
    
    @Override
    public ShardLock acquireLock(String shardName) throws BusyLockException {
        if (StringUtils.isBlank(shardName)) {
            throw new BusyLockException("Cannot lock on empty or null path");
        }
        String path = PATH_PREFIX + shardName;
        LOG.debug("Acquiring lock at path " + path);
        try {
            InterProcessMutex mutex = prepareMutex(curator, path);
            if (!mutex.acquire(MAX_MUTEX_WAIT_TIME_MS, TimeUnit.MILLISECONDS)) {
                throw new BusyLockException("Cannot acquire lock on path " + path);
            }
            LOG.debug("Acquired lock at path " + path);
            return new ZkShardLock(mutex, path, shardName);
        } catch (Exception ex) {
            LOG.warn("Error getting lock on path " + path + ":" + ex.getMessage());
            throw new BusyLockException("Error getting lock on path " + path + ":" + ex.getMessage());
        }
    }

    @Override
    public void releaseLock(ShardLock lock) {
        ZkShardLock lzk = (ZkShardLock)lock;
        try {
            if (lock != null && lzk.getMutex() != null) {
                LOG.debug("Releasing lock at path " + lzk.getLockPath());
                if (lzk.getMutex().isAcquiredInThisProcess()) {
                    lzk.getMutex().release();
                    LOG.info("Lock at path " + lzk.getLockPath() + " released");
                } else {
                    LOG.info("Lock at path " + lzk.getLockPath() + " is not owned by current thread.");
                }
            }
        } catch (Exception e) {
            LOG.warn("Error releasing lock for path " + lzk.getLockPath() + ":" + e.getMessage());
        }

    }

    /**
     * Prepares and returns an interprocess mutex.
     *
     * @param cf The locking client
     * @param lockPath The lock path
     * @return An interprocess mutex
     */
    InterProcessMutex prepareMutex(CuratorFramework cf, String lockPath) {
        return new InterProcessMutex(cf, lockPath);
    }
    
}
