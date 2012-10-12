package com.netflix.astyanax.connectionpool.impl;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.StringUtils;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy;

/**
 * Partition hosts by start token.  Each token may map to a list of partitions.
 * 
 * @author elandau
 *
 * @param <CL>
 */
public class TokenPartitionedTopology<CL> implements Topology<CL> {
    /**
     * Sorted list of partitions.  A binary search is performed on this list to determine
     * the list of hosts that own the token range.
     */
    private AtomicReference<List<HostConnectionPoolPartition<CL>>> sortedRing
    	= new AtomicReference<List<HostConnectionPoolPartition<CL>>>();

    /**
     * Lookup of start token to partition 
     */
    private NonBlockingHashMap<BigInteger, HostConnectionPoolPartition<CL>> tokens
    	= new NonBlockingHashMap<BigInteger, HostConnectionPoolPartition<CL>>();

    /**
     * Partition which contains all hosts.  This is the fallback partition when no tokens are provided.
     */
    private HostConnectionPoolPartition<CL> allPools;

    /**
     * Strategy used to score hosts within a partition.
     */
    private LatencyScoreStrategy strategy;

    /**
     * Comparator used to find the partition mapping to a token
     */
    @SuppressWarnings("rawtypes")
    private Comparator tokenSearchComparator = new Comparator() {
        @SuppressWarnings("unchecked")
        @Override
        public int compare(Object arg0, Object arg1) {
            HostConnectionPoolPartition<CL> partition = (HostConnectionPoolPartition<CL>) arg0;
            BigInteger token = (BigInteger) arg1;
            return partition.id().compareTo(token);
        }
    };

    /**
     * Compartor used to sort partitions in token order.  
     */
    @SuppressWarnings("rawtypes")
    private Comparator partitionComparator = new Comparator() {
        @SuppressWarnings("unchecked")
        @Override
        public int compare(Object arg0, Object arg1) {
            HostConnectionPoolPartition<CL> partition0 = (HostConnectionPoolPartition<CL>) arg0;
            HostConnectionPoolPartition<CL> partition1 = (HostConnectionPoolPartition<CL>) arg1;
            return partition0.id().compareTo(partition1.id());
        }
    };

    public TokenPartitionedTopology(LatencyScoreStrategy strategy) {
        this.strategy = strategy;
        this.allPools = new HostConnectionPoolPartition<CL>(null, this.strategy);
    }

    protected HostConnectionPoolPartition<CL> makePartition(BigInteger partition) {
        return new HostConnectionPoolPartition<CL>(partition, strategy);
    }

    @SuppressWarnings("unchecked")
    @Override
    /**
     * Update the list of pools using the provided mapping of start token to collection of hosts
     * that own the token
     */
    public synchronized boolean setPools(Map<BigInteger, Collection<HostConnectionPool<CL>>> ring) {
        // Temporary list of token that will be removed if not found in the new ring
        Set<BigInteger> tokensToRemove = Sets.newHashSet(tokens.keySet());

        Set<HostConnectionPool<CL>> allPools = Sets.newHashSet();

        boolean didChange = false;
        // Iterate all tokens
        for (Entry<BigInteger, Collection<HostConnectionPool<CL>>> entry : ring.entrySet()) {
            BigInteger token = entry.getKey();

            if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                tokensToRemove.remove(token);
                
                // Always add to the all pools
                if (allPools.addAll(entry.getValue()))
                    didChange = true;

                // Add a new partition or modify an existing one
                HostConnectionPoolPartition<CL> partition = tokens.get(token);
                if (partition == null) {
                    partition = makePartition(token);
                    tokens.put(token, partition);
                }
                if (partition.setPools(entry.getValue()))
                    didChange = true;
            }
        }

        // Remove the tokens that are no longer in the ring
        for (BigInteger token : tokensToRemove) {
            tokens.remove(token);
            didChange = true;
        }

        // Sort partitions by token
        if (didChange) {
            List<HostConnectionPoolPartition<CL>> partitions = Lists.newArrayList(tokens.values());
            Collections.sort(partitions, partitionComparator);
            this.allPools.setPools(allPools);
            refresh();
            
            this.sortedRing.set(Collections.unmodifiableList(partitions));
        }

        return didChange;
    }

    @Override
    public synchronized void resumePool(HostConnectionPool<CL> pool) {
        refresh();
    }

    @Override
    public synchronized void suspendPool(HostConnectionPool<CL> pool) {
        refresh();
    }

    @Override
    public synchronized void refresh() {
        for (HostConnectionPoolPartition<CL> partition : tokens.values()) {
            partition.refresh();
        }
        allPools.refresh();
    }

    @Override
    public HostConnectionPoolPartition<CL> getPartition(BigInteger token) {
        // First, get a copy of the partitions.
    	List<HostConnectionPoolPartition<CL>> partitions = this.sortedRing.get();
        // Must have a token otherwise we default to the base class
        // implementation
        if (token == null || partitions == null || partitions.isEmpty()) {
            return getAllPools();
        }

        // Do a binary search to find the token partition which owns the
        // token. We can get two responses here.
        // 1. The token is in the list in which case the response is the
        // index of the partition
        // 2. The token is not in the list in which case the response is the
        // index where the token would have been inserted into the list.
        // We convert this index (which is negative) to the index of the
        // previous position in the list.
        @SuppressWarnings("unchecked")
        int partitionIndex = Collections.binarySearch(partitions, token, tokenSearchComparator);
        if (partitionIndex < 0) {
            partitionIndex = -(partitionIndex + 1);
        }
        return partitions.get(partitionIndex % partitions.size());
    }

    @Override
    public HostConnectionPoolPartition<CL> getAllPools() {
        return allPools;
    }

    @Override
    public int getPartitionCount() {
        return tokens.size();
    }

    @Override
    public synchronized void removePool(HostConnectionPool<CL> pool) {
        allPools.removePool(pool);
        for (HostConnectionPoolPartition<CL> partition : tokens.values()) {
            partition.removePool(pool);
        }
        refresh();
    }

    @Override
    public synchronized void addPool(HostConnectionPool<CL> pool) {
        allPools.addPool(pool);
        allPools.refresh();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("TokenPartitionTopology[");
        sb.append(StringUtils.join(this.sortedRing.get(), ","));
        sb.append(", RING:").append(this.allPools);
        sb.append("]");
        return sb.toString();
    }

}
