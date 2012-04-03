package com.netflix.astyanax.connectionpool.impl;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.cliffc.high_scale_lib.NonBlockingHashSet;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy;

public class HostConnectionPoolPartition<CL> {

    private final BigInteger id;
    private final AtomicBoolean prioritize = new AtomicBoolean(false);
    private final NonBlockingHashSet<HostConnectionPool<CL>> pools = new NonBlockingHashSet<HostConnectionPool<CL>>();
    private final AtomicReference<List<HostConnectionPool<CL>>> activePools = new AtomicReference<List<HostConnectionPool<CL>>>();
    private final LatencyScoreStrategy strategy;

    public HostConnectionPoolPartition(BigInteger id,
            LatencyScoreStrategy strategy) {
        this.id = id;
        this.strategy = strategy;
        this.activePools.set(new ArrayList<HostConnectionPool<CL>>());
    }

    /**
     * Sets all pools for this partition. Removes old partitions and adds new
     * one.
     * 
     * @param pools
     */
    public synchronized boolean setPools(
            Collection<HostConnectionPool<CL>> pools) {
        Set<HostConnectionPool<CL>> toRemove = Sets.newHashSet(this.pools);

        boolean didChange = false;
        for (HostConnectionPool<CL> pool : pools) {
            didChange |= this.pools.add(pool);
            toRemove.remove(pool);
        }

        for (HostConnectionPool<CL> pool : toRemove) {
            didChange |= this.pools.remove(pool);
        }

        if (didChange)
            refresh();
        return didChange;
    }

    public synchronized boolean addPool(HostConnectionPool<CL> pool) {
        if (this.pools.add(pool)) {
            refresh();
            return true;
        }
        return false;
    }

    public synchronized boolean removePool(HostConnectionPool<CL> pool) {
        if (this.pools.remove(pool)) {
            refresh();
            return true;
        }
        return false;
    }

    /**
     * Token or shard identifying this partition.
     * 
     * @return
     */
    public BigInteger id() {
        return this.id;
    }

    public List<HostConnectionPool<CL>> getPools() {
        return this.activePools.get();
    }

    public boolean isSorted() {
        return prioritize.get();
    }

    public void refresh() {
        List<HostConnectionPool<CL>> pools = Lists.newArrayList(this.pools);
        Iterator<HostConnectionPool<CL>> iter = pools.iterator();
        while (iter.hasNext()) {
            if (iter.next().isShutdown())
                iter.remove();
        }
        this.activePools
                .set(strategy.sortAndfilterPartition(pools, prioritize));
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("HostConnectionPoolPartition[");
        sb.append(id).append(": ");
        for (HostConnectionPool<CL> pool : getPools()) {
            sb.append(pool.getHost().getHostName()).append(",");
        }
        sb.append("]");
        return sb.toString();
    }
}