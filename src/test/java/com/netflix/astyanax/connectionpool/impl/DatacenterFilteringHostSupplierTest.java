package com.netflix.astyanax.connectionpool.impl;

import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.impl.DatacenterFilteringHostSupplier;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class DatacenterFilteringHostSupplierTest {

    private final Host localNode = new Host("127.0.0.1", 9160, "localDC", "rack");
    private final Host remoteNode = new Host("127.0.0.2", 9160, "remoteDC", "rack");

    Supplier<Map<BigInteger, List<Host>>> sourceSupplier = new Supplier<Map<BigInteger, List<Host>>>() {
        @Override
        public Map<BigInteger, List<Host>> get() {
            Map<BigInteger, List<Host>> hosts = Maps.newHashMap();
            hosts.put(new BigInteger("1"), Lists.newArrayList(localNode));
            hosts.put(new BigInteger("2"), Lists.newArrayList(remoteNode));
            return hosts;
        }
    };

    Set<Host> filteredHosts;

    @Before
    public void setUp() throws Exception {
        List<Host> initialHosts = Lists.newArrayList(new Host(localNode.getUrl(), 9160));
        DatacenterFilteringHostSupplier supplier = new DatacenterFilteringHostSupplier(initialHosts, sourceSupplier);
        Map<BigInteger, List<Host>> ring = supplier.get();
        filteredHosts = Sets.newHashSet(Iterables.concat(ring.values()));
    }

    @Test
    public void testLocalNodeIsIncluded() throws Exception {
        Assert.assertTrue(filteredHosts.contains(localNode));
    }

    @Test
    public void testRemoteNodeIsExcluded() throws Exception {
        Assert.assertFalse(filteredHosts.contains(remoteNode));
    }
}
