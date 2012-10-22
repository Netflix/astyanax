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
import com.netflix.astyanax.partitioner.LongBOPPartitioner;
import com.netflix.astyanax.test.TestClient;
import com.netflix.astyanax.test.TestHostConnectionPool;

public class HostConnectionPoolPartitionTest {

    @Test
    public void testPartition() {
        LatencyScoreStrategy strategy = new SmaLatencyScoreStrategyImpl(10000, 60000, 100, 4.0);

        TokenHostConnectionPoolPartition partition = new TokenHostConnectionPoolPartition(new BigInteger("1"), strategy);

        List<TestHostConnectionPool> pools = Arrays.asList(
                makePool(1),
                makePool(2), 
                makePool(3), 
                makePool(4));

        List<TestHostConnectionPool> pool1 = Arrays.asList(
                pools.get(0),
                pools.get(1), 
                pools.get(2));
        List<TestHostConnectionPool> pool2 = Arrays.asList(
                pools.get(0),
                pools.get(3));

        Assert.assertEquals(0, partition.getPools().size());
        partition.setPools(pool1);
        Assert.assertEquals(3, partition.getPools().size());

        partition.setPools(pool2);
        Assert.assertEquals(2, partition.getPools().size());
    }

    // Is there a reason that this test is in HostConnectionPoolPartitionTest?
    // Perhaps this should be moved to the existing TokenAwareConnectionPoolTest or
    // a new TokenParitionedTopologyTest?
    @Test
    public void testTopology() {
        LatencyScoreStrategy strategy = new SmaLatencyScoreStrategyImpl(10000,60000, 100, 4.0);

        int nHosts = 6;
        int nReplicationFactor = 3;

        TokenPartitionedTopology<TestClient> topology = new TokenPartitionedTopology<TestClient>(LongBOPPartitioner.get(), strategy);

        // Make the set of pools
        List<HostConnectionPool<TestClient>> pools = Lists.newArrayList();
        for (int i = 0; i < nHosts; i++) {
            pools.add(makePool(i));
        }

        // Make the flat ring (before ring_describe is called)
        List<HostConnectionPool<TestClient>> flatRing = pools;
        boolean didChange = topology.setPools(flatRing);
        Assert.assertTrue(didChange);
        Assert.assertEquals(0, topology.getPartitionCount());
        System.out.println(topology);

        // Make a ring with tokens and RF
//        Map<BigInteger, Collection<HostConnectionPool<TestClient>>> tokenRing = Maps.newHashMap();
//        for (int i = 0; i < nHosts; i++) {
//            List<HostConnectionPool<TestClient>> partition = Lists.newArrayList();
//            for (int j = 0; j < nReplicationFactor; j++) {
//                partition.add(pools.get((i + j) % nHosts));
//            }
//            tokenRing.put(new BigInteger(Integer.toString(i * 1000)), partition);
//        }
//
//        didChange = topology.setPools(tokenRing);
//        Assert.assertTrue(didChange);
//        Assert.assertEquals(nHosts, topology.getPartitionCount());
//        System.out.println(topology);
//
//        HostConnectionPoolPartition<TestClient> partition = topology
//                .getPartition(null);
//        Assert.assertEquals(nHosts, partition.getPools().size());
//
//        // Partition Token Map:
//        // HCP 0 - (5000, 0]
//        // HCP 1000 - (0, 1000]
//        // HCP 2000 - (1000, 2000]
//        // HCP 3000 - (2000, 3000]
//        // HCP 4000 - (3000, 4000]
//        // HCP 5000 - (4000, 5000]
//
//        // Test ordinals
//        for (int i = 0; i < nHosts; i++) {
//            partition = topology.getPartition(new BigInteger(Integer
//                    .toString(i * 1000)));
//            Assert.assertEquals(new BigInteger(Integer.toString(i * 1000)),
//                    partition.id());
//        }
//
//        // Test mid-range tokens
//        for (int i = nHosts; i > 0; i--) {
//            partition = topology.getPartition(new BigInteger(Integer
//                    .toString(i * 1000 - 500)));
//
//            if (i == nHosts) {  // 5500 is contained in (5000,0] which belongs to HCP 0
//                Assert.assertEquals(BigInteger.ZERO, partition.id());
//            } else {
//                Assert.assertEquals(new BigInteger(Integer.toString(i * 1000)),
//                        partition.id());
//            }
//        }
//
//        Map<BigInteger, Collection<HostConnectionPool<TestClient>>> emptyRing = Maps
//                .newHashMap();
//        topology.setPools(emptyRing);
//        System.out.println(topology);
//        Assert.assertEquals(0, topology.getPartitionCount());
//        Assert.assertEquals(0, topology.getAllPools().getPools().size());
    }

    public TestHostConnectionPool makePool(int index) {
        return new TestHostConnectionPool(new Host("127.0.0." + index, 0));
    }
}
