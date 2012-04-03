package com.netflix.astyanax.connectionpool.impl;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Map;

import com.netflix.astyanax.connectionpool.HostConnectionPool;

public interface Topology<CL> {
    /**
     * Refresh the internal topology structure
     * 
     * @param ring
     * @return
     */
    boolean setPools(Map<BigInteger, Collection<HostConnectionPool<CL>>> ring);

    /**
     * Add a pool without knowing it's token. This pool will be added to the all
     * pools partition only
     * 
     * @param pool
     */
    void addPool(HostConnectionPool<CL> pool);

    /**
     * Remove this pool from all partitions
     * 
     * @param pool
     */
    void removePool(HostConnectionPool<CL> pool);

    /**
     * Resume a host that was previously down
     * 
     * @param pool
     */
    void resumePool(HostConnectionPool<CL> pool);

    /**
     * Suspend a host that is down
     * 
     * @param pool
     */
    void suspendPool(HostConnectionPool<CL> pool);

    /**
     * Refresh the internal state and apply the latency score strategy
     */
    void refresh();

    /**
     * Search for the partition that owns this token
     * 
     * @param token
     * @return
     */
    HostConnectionPoolPartition<CL> getPartition(BigInteger token);

    /**
     * Return a partition that represents all hosts in the ring
     * 
     * @return
     */
    HostConnectionPoolPartition<CL> getAllPools();

    /**
     * Get total number of tokens in the topology
     * 
     * @return
     */
    int getPartitionCount();
}
