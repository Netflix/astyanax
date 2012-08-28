package com.netflix.astyanax.impl;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.astyanax.*;
import com.netflix.astyanax.connectionpool.*;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.test.TestEndpoint;
import com.netflix.astyanax.test.TestKeyspace;
import com.netflix.astyanax.test.TestTokenRange;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;

public class NetworkTopologyAwarenessTest {

    private final Endpoint localEndpoint1 = new TestEndpoint("127.0.0.1", "localDC", "rack1");
    private final Endpoint localEndpoint2 = new TestEndpoint("127.0.0.2", "localDC", "rack2");
    private final Endpoint remoteEndpoint = new TestEndpoint("127.0.0.3", "remoteDC", "rack3");

    Set<String> hosts;

    @Before
    public void setUp() throws Exception {

        final TestKeyspace mockKeyspace = new TestKeyspace("foobar");
        mockKeyspace.setTokenRange(Lists.<TokenRange>newArrayList(
                new TestTokenRange("0", "1", Lists.newArrayList(localEndpoint1, localEndpoint2, remoteEndpoint))));

        AstyanaxContext<Keyspace> keyspace = new AstyanaxContext.Builder()
                .forCluster("foobar")
                .forKeyspace("foobar")
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("local")
                        .setSeeds(localEndpoint1.getHost())
                )
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                        .setDiscoveryType(NodeDiscoveryType.TOKEN_AWARE)
                        .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE)
                        .setNetworkTopologyAware(true)
                )
                .buildKeyspace(new AstyanaxTypeFactory<Keyspace>() {
                    @Override
                    public Keyspace createKeyspace(String ksName, ConnectionPool<Keyspace> cp, AstyanaxConfiguration asConfig, KeyspaceTracerFactory tracerFactory) {
                        return mockKeyspace;
                    }

                    @Override
                    public Cluster createCluster(ConnectionPool<Keyspace> cp, AstyanaxConfiguration asConfig, KeyspaceTracerFactory tracerFactory) {
                        return null;
                    }

                    @Override
                    public ConnectionFactory<Keyspace> createConnectionFactory(AstyanaxConfiguration asConfig, ConnectionPoolConfiguration cfConfig, KeyspaceTracerFactory tracerFactory, ConnectionPoolMonitor monitor) {
                        return null;
                    }
                });

        keyspace.start();

        @SuppressWarnings("unchecked")
        List<HostConnectionPool<Keyspace>> pools = ((ConnectionPool<Keyspace>)keyspace.getConnectionPool()).getActivePools();

        keyspace.shutdown();

        hosts = Sets.newHashSet(Lists.transform(pools, new Function<HostConnectionPool<Keyspace>, String>() {
            @Override
            public String apply(HostConnectionPool<Keyspace> pool) {
                return pool.getHost().getIpAddress();
            }
        }));
    }

    @Test
    public void testLocalNodesAreIncluded() throws Exception {
        Assert.assertTrue(hosts.contains(localEndpoint1.getHost()));
        Assert.assertTrue(hosts.contains(localEndpoint2.getHost()));
    }

    @Test
    public void testRemoteNodeIsExcluded() throws Exception {
        Assert.assertFalse(hosts.contains(remoteEndpoint.getHost()));
    }

}
