package com.netflix.astyanax.recipes.queue.zookeeper;

import com.netflix.astyanax.recipes.queue.ShardLock;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;

/**
 *
 * @author pbhattacharyya
 */
public class ZkShardLock implements ShardLock {


    private InterProcessMutex mutex;
    private String lockPath;
    private String shardName;

    public ZkShardLock(InterProcessMutex mutex, String lockPath, String shardName) {
        this.mutex = mutex;
        this.lockPath = lockPath;
        this.shardName = shardName;
    }

    public InterProcessMutex getMutex() {
        return mutex;
    }

    public void setMutex(InterProcessMutex mutex) {
        this.mutex = mutex;
    }

    public String getLockPath() {
        return lockPath;
    }

    public void setLockPath(String lockPath) {
        this.lockPath = lockPath;
    }

    public String getShardName() {
        return shardName;
    }

    public void setShardName(String shardName) {
        this.shardName = shardName;
    }

}
