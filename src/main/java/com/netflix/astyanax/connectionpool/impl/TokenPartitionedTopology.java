package com.netflix.astyanax.connectionpool.impl;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy;

public class TokenPartitionedTopology<CL> implements Topology<CL> {
    private AtomicReference<List<HostConnectionPoolPartition<CL>>> sortedRing = new AtomicReference<List<HostConnectionPoolPartition<CL>>>();

    private Map<BigInteger, HostConnectionPoolPartition<CL>> tokens = Maps
            .newHashMap();

    private HostConnectionPoolPartition<CL> allPools;

    private LatencyScoreStrategy strategy;

    /**
     * Comparator used to find the connection pool to the host which owns a
     * specific token
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
    public synchronized boolean setPools(
            Map<BigInteger, Collection<HostConnectionPool<CL>>> ring) {
        // Temporary list of token that will be removed if not found in the new
        // ring
        Set<BigInteger> tokensToRemove = Sets.newHashSet(tokens.keySet());

        Set<HostConnectionPool<CL>> allPools = Sets.newHashSet();
        ;

        boolean didChange = false;
        // Iterate all tokens
        for (Entry<BigInteger, Collection<HostConnectionPool<CL>>> entry : ring
                .entrySet()) {
            BigInteger token = entry.getKey();
            tokensToRemove.remove(token);

            didChange |= allPools.addAll(entry.getValue());

            if (entry.getValue() != null) {
                // Add a new collection or modify an existing one
                HostConnectionPoolPartition<CL> partition = tokens.get(token);
                if (partition == null) {
                    partition = makePartition(token);
                    tokens.put(token, partition);
                }
                didChange |= partition.setPools(entry.getValue());
            }
        }

        // Remove the tokens that are no longer in the ring
        didChange |= !tokensToRemove.isEmpty();
        for (BigInteger token : tokensToRemove) {
            tokens.remove(token);
        }

        // Sort partitions by token
        if (didChange) {
            List<HostConnectionPoolPartition<CL>> partitions = new ArrayList<HostConnectionPoolPartition<CL>>(
                    tokens.values());
            Collections.sort(partitions, partitionComparator);
            this.allPools.setPools(allPools);
            refresh();
            this.sortedRing.set(partitions);
        }

        return didChange;
    }

    @Override
    public void resumePool(HostConnectionPool<CL> pool) {
        refresh();
    }

    @Override
    public void suspendPool(HostConnectionPool<CL> pool) {
        refresh();
    }

    @Override
    public void refresh() {
        for (HostConnectionPoolPartition<CL> partition : tokens.values()) {
            partition.refresh();
        }
        allPools.refresh();
    }

    @Override
    public HostConnectionPoolPartition<CL> getPartition(BigInteger token) {
        // First, get a copy of the partitions.
        List<HostConnectionPoolPartition<CL>> partitions = this.sortedRing
                .get();
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
        int j = Collections.binarySearch(partitions, token,
                tokenSearchComparator);
        if (j < 0) {
            j = -j - 2;
        }

        return partitions.get(j % partitions.size());
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
    public void removePool(HostConnectionPool<CL> pool) {
        allPools.removePool(pool);
        for (HostConnectionPoolPartition<CL> partition : tokens.values()) {
            partition.removePool(pool);
        }
        refresh();
    }

    @Override
    public void addPool(HostConnectionPool<CL> pool) {
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
