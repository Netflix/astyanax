/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.astyanax.connectionpool.impl;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy;

public class AbstractTopology<CL> implements Topology<CL> {
    /**
     * Partition which contains all hosts.  This is the fallback partition when no tokens are provided.
     */
    private TokenHostConnectionPoolPartition<CL> allPools;

    /**
     * Strategy used to score hosts within a partition.
     */
    private LatencyScoreStrategy strategy;

    public AbstractTopology(LatencyScoreStrategy strategy) {
        this.strategy = strategy;
        this.allPools = new TokenHostConnectionPoolPartition<CL>(null, this.strategy);
    }

    @SuppressWarnings("unchecked")
    @Override
    /**
     * Update the list of pools using the provided mapping of start token to collection of hosts
     * that own the token
     */
    public synchronized boolean setPools(Collection<HostConnectionPool<CL>> ring) {
        boolean didChange = false;
        Set<HostConnectionPool<CL>> allPools = Sets.newHashSet();
        
        // Create a mapping of end token to a list of hosts that own the token
        for (HostConnectionPool<CL> pool : ring) {
            allPools.add(pool);
            if (!this.allPools.hasPool(pool))
                didChange = true;
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
        allPools.refresh();
    }

    @Override
    public TokenHostConnectionPoolPartition<CL> getPartition(ByteBuffer rowkey) {
        return getAllPools();
    }

    @Override
    public TokenHostConnectionPoolPartition<CL> getAllPools() {
        return allPools;
    }

    @Override
    public int getPartitionCount() {
        return 1;
    }

    @Override
    public synchronized void removePool(HostConnectionPool<CL> pool) {
        allPools.removePool(pool);
        refresh();
    }

    @Override
    public synchronized void addPool(HostConnectionPool<CL> pool) {
        allPools.addPool(pool);
        allPools.refresh();
    }

    @Override
    public List<String> getPartitionNames() {
        return Lists.newArrayList(allPools.id().toString());
    }

    @Override
    public TokenHostConnectionPoolPartition<CL> getPartition(String token) {
        return allPools;
    }

    @Override
    public Map<String, TokenHostConnectionPoolPartition<CL>> getPartitions() {
        return ImmutableMap.of(allPools.id().toString(), allPools);
    }
}
