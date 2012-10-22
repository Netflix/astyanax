package com.netflix.astyanax.connectionpool.impl;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.cliffc.high_scale_lib.NonBlockingHashSet;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy;

/**
 * Collection of hosts that are grouped by a certain criteria (such as token or rack)
 * 
 * @author elandau
 *
 * @param <CL>
 */
public class HostConnectionPoolPartition<CL> {
    protected final AtomicBoolean                                 prioritize  = new AtomicBoolean(false);
    protected final NonBlockingHashSet<HostConnectionPool<CL>>    pools       = new NonBlockingHashSet<HostConnectionPool<CL>>();
    protected final AtomicReference<List<HostConnectionPool<CL>>> activePools = new AtomicReference<List<HostConnectionPool<CL>>>();
    protected final LatencyScoreStrategy                          strategy;
    
    public HostConnectionPoolPartition(LatencyScoreStrategy strategy) {
        this.strategy = strategy;
        this.activePools.set(Lists.<HostConnectionPool<CL>>newArrayList());
    }
    
    /**
     * Sets all pools for this partition. Removes old partitions and adds new
     * one.
     * 
     * @param newPools
     */
    public synchronized boolean setPools(Collection<HostConnectionPool<CL>> newPools) {
        Set<HostConnectionPool<CL>> toRemove = Sets.newHashSet(this.pools);
        
        // Add new pools not previously seen
        boolean didChange = false;
        for (HostConnectionPool<CL> pool : newPools) {
            if (this.pools.add(pool))   
                didChange = true;
            toRemove.remove(pool);
        }
    
        // Remove pools for hosts that no longer exist
        for (HostConnectionPool<CL> pool : toRemove) {
            if (this.pools.remove(pool))
                didChange = true;
        }
    
        if (didChange)
            refresh();
        return didChange;
    }
    
    /**
     * Add a new pool to the partition.  Checks to see if the pool already
     * existed.  If so then there is no need to refresh the pool.
     * @param pool
     * @return
     */
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
     * Return the list of active hosts.  Active hosts are those deemed by the 
     * latency score strategy to be alive and responsive.  
     * @return
     */
    public List<HostConnectionPool<CL>> getPools() {
        return activePools.get();
    }
    
    /**
     * If true the the hosts are sorted by order of priority where the 
     * first host gives the best performance
     * @return
     */
    public boolean isSorted() {
        return prioritize.get();
    }
    
    /**
     * Returns true if a pool is contained in this partition
     * @param pool
     * @return
     */
    public boolean hasPool(HostConnectionPool<CL> pool) {
        return pools.contains(pool);
    }
    
    /**
     * Refresh the partition 
     */
    public synchronized void refresh() {
        List<HostConnectionPool<CL>> pools = Lists.newArrayList();
        for (HostConnectionPool<CL> pool : this.pools) {
            if (!pool.isReconnecting()) {
                pools.add(pool);
            }
        }
        this.activePools.set(strategy.sortAndfilterPartition(pools, prioritize));
    }
    
    public String toString() {
        return new StringBuilder()
            .append("BaseHostConnectionPoolPartition[")   
            .append(StringUtils.join(Collections2.transform(getPools(), new Function<HostConnectionPool<CL>, String>() {
                @Override
                public String apply(@Nullable HostConnectionPool<CL> host) {
                    return host.getHost().getHostName();
                }
            }), ","))
            .append("]")
            .toString();
    }

}
