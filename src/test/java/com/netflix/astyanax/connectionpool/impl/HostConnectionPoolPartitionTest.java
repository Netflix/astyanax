package com.netflix.astyanax.connectionpool.impl;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy;
import com.netflix.astyanax.fake.TestClient;
import com.netflix.astyanax.fake.TestHostConnectionPool;

public class HostConnectionPoolPartitionTest {

    @Test
    public void testPartition() {
        LatencyScoreStrategy strategy = new SmaLatencyScoreStrategyImpl(10000,
                60000, 100, 4.0);

        HostConnectionPoolPartition partition = new HostConnectionPoolPartition(
                new BigInteger("1"), strategy);

        List<TestHostConnectionPool> pools = Arrays.asList(makePool(1),
                makePool(2), makePool(3), makePool(4));

        List<TestHostConnectionPool> pool1 = Arrays.asList(pools.get(0),
                pools.get(1), pools.get(2));
        List<TestHostConnectionPool> pool2 = Arrays.asList(pools.get(0),
                pools.get(3));

        Assert.assertEquals(0, partition.getPools().size());
        partition.setPools(pool1);
        Assert.assertEquals(3, partition.getPools().size());

        partition.setPools(pool2);
        Assert.assertEquals(2, partition.getPools().size());
    }

    @Test
    public void testTopology() {
        LatencyScoreStrategy strategy = new SmaLatencyScoreStrategyImpl(10000,
                60000, 100, 4.0);

        int nHosts = 6;
        int nReplicationFactor = 3;

        TokenPartitionedTopology<TestClient> topology = new TokenPartitionedTopology<TestClient>(
                strategy);

        // Make the set of pools
        List<HostConnectionPool<TestClient>> pools = Lists.newArrayList();
        for (int i = 0; i < nHosts; i++) {
            pools.add(makePool(i));
        }

        // Make the flat ring (before ring_describe is called)
        Map<BigInteger, Collection<HostConnectionPool<TestClient>>> flatRing = Maps
                .newHashMap();
        flatRing.put(new BigInteger("0"), pools);
        boolean didChange = topology.setPools(flatRing);
        Assert.assertTrue(didChange);
        Assert.assertEquals(1, topology.getPartitionCount());
        System.out.println(topology);

        // Make a ring with tokens and RF
        Map<BigInteger, Collection<HostConnectionPool<TestClient>>> tokenRing = Maps
                .newHashMap();
        for (int i = 0; i < nHosts; i++) {
            List<HostConnectionPool<TestClient>> partition = Lists
                    .newArrayList();
            for (int j = 0; j < nReplicationFactor; j++) {
                partition.add(pools.get((i + j) % nHosts));
            }
            tokenRing
                    .put(new BigInteger(Integer.toString(i * 1000)), partition);
        }

        didChange = topology.setPools(tokenRing);
        Assert.assertTrue(didChange);
        Assert.assertEquals(nHosts, topology.getPartitionCount());
        System.out.println(topology);

        HostConnectionPoolPartition<TestClient> partition = topology
                .getPartition(null);
        Assert.assertEquals(nHosts, partition.getPools().size());

        for (int i = 0; i < nHosts; i++) {
            partition = topology.getPartition(new BigInteger(Integer
                    .toString(i * 1000)));
            Assert.assertEquals(new BigInteger(Integer.toString(i * 1000)),
                    partition.id());
        }

        for (int i = 0; i < nHosts; i++) {
            partition = topology.getPartition(new BigInteger(Integer
                    .toString(i * 1000 + 500)));
            Assert.assertEquals(new BigInteger(Integer.toString(i * 1000)),
                    partition.id());
        }

        Map<BigInteger, Collection<HostConnectionPool<TestClient>>> emptyRing = Maps
                .newHashMap();
        topology.setPools(emptyRing);
        System.out.println(topology);
        Assert.assertEquals(0, topology.getPartitionCount());
        Assert.assertEquals(0, topology.getAllPools().getPools().size());
    }

    public TestHostConnectionPool makePool(int index) {
        return new TestHostConnectionPool(new Host("127.0.0." + index, 0));
    }
}
