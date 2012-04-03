package com.netflix.astyanax.connectionpool.impl;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.HostDownException;
import com.netflix.astyanax.connectionpool.exceptions.PoolTimeoutException;
import com.netflix.astyanax.fake.TestClient;
import com.netflix.astyanax.fake.TestConnectionFactory;
import com.netflix.astyanax.fake.TestHostType;

public class SimpleHostConnectionPoolTest {
    private static Logger LOG = LoggerFactory
            .getLogger(SimpleHostConnectionPoolTest.class);

    private static int WAIT_TIMEOUT = 100;

    public static class NoOpListener implements
            SimpleHostConnectionPool.Listener<TestClient> {
        @Override
        public void onHostDown(HostConnectionPool<TestClient> pool) {
        }

        @Override
        public void onHostUp(HostConnectionPool<TestClient> pool) {
        }

    }

    @Test
    public void testAddHost() {
        Host host = new Host("127.0.0.1", TestHostType.GOOD_FAST.ordinal());

        CountingConnectionPoolMonitor monitor = new CountingConnectionPoolMonitor();

        ConnectionPoolConfiguration config = createConfig();
        SimpleHostConnectionPool<TestClient> pool = new SimpleHostConnectionPool<TestClient>(
                host, new TestConnectionFactory(config, monitor), monitor,
                config, new NoOpListener());
        Assert.assertEquals(0, pool.getActiveConnectionCount());
        Assert.assertEquals(false, pool.isShutdown());
        Assert.assertEquals(0, pool.getIdleConnectionCount());

        try {
            pool.growConnections(1);
        } catch (ConnectionException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        }
        Assert.assertEquals(1, pool.getActiveConnectionCount());
        Assert.assertEquals(false, pool.isShutdown());
        Assert.assertEquals(1, pool.getIdleConnectionCount());
    }

    @Test
    public void testAddHostWithCheckedException() {
        Host host = new Host("127.0.0.1",
                TestHostType.CONNECT_TIMEOUT.ordinal());

        ConnectionPoolConfiguration config = createConfig();
        CountingConnectionPoolMonitor monitor = new CountingConnectionPoolMonitor();
        SimpleHostConnectionPool<TestClient> pool = new SimpleHostConnectionPool<TestClient>(
                host, new TestConnectionFactory(config, monitor), monitor,
                config, new NoOpListener());

        try {
            pool.growConnections(1);
            Assert.fail();
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        } catch (ConnectionException e) {
        }
        Assert.assertEquals(0, pool.getActiveConnectionCount());
        Assert.assertEquals(true, pool.isShutdown());
        Assert.assertEquals(0, pool.getIdleConnectionCount());
    }

    @Test
    public void testAddHostWithUncheckedException() {
        Host host = new Host("127.0.0.1",
                TestHostType.CONNECT_WITH_UNCHECKED_EXCEPTION.ordinal());

        ConnectionPoolConfiguration config = createConfig();
        CountingConnectionPoolMonitor monitor = new CountingConnectionPoolMonitor();
        SimpleHostConnectionPool<TestClient> pool = new SimpleHostConnectionPool<TestClient>(
                host, new TestConnectionFactory(config, monitor), monitor,
                config, new NoOpListener());

        try {
            pool.growConnections(1);
            Assert.fail();
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        } catch (ConnectionException e) {
        }
        Assert.assertEquals(0, pool.getActiveConnectionCount());
        Assert.assertEquals(true, pool.isShutdown());
        Assert.assertEquals(0, pool.getIdleConnectionCount());
    }

    @Test
    public void testGrowConnections() {
        Host host = new Host("127.0.0.1", TestHostType.GOOD_FAST.ordinal());

        ConnectionPoolConfiguration config = createConfig();
        CountingConnectionPoolMonitor monitor = new CountingConnectionPoolMonitor();
        SimpleHostConnectionPool<TestClient> pool = new SimpleHostConnectionPool<TestClient>(
                host, new TestConnectionFactory(config, monitor), monitor,
                config, new NoOpListener());

        try {
            pool.growConnections(2);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        } catch (ConnectionException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        }
        Assert.assertEquals(2, pool.getActiveConnectionCount());
        Assert.assertEquals(false, pool.isShutdown());
        Assert.assertEquals(2, pool.getIdleConnectionCount());

        try {
            pool.growConnections(2);
            Assert.fail();
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        } catch (ConnectionException e) {
        }
        Assert.assertEquals(2, pool.getActiveConnectionCount());
        Assert.assertEquals(false, pool.isShutdown());
    }

    @Test
    public void testShutdown() {
        Host host = new Host("127.0.0.1", TestHostType.GOOD_FAST.ordinal());

        ConnectionPoolConfigurationImpl config = createConfig();
        config.setRetryBackoffStrategy(new FixedRetryBackoffStrategy(200, 2000));
        config.setMaxConnsPerHost(3);
        config.setMaxPendingConnectionsPerHost(2);
        CountingConnectionPoolMonitor monitor = new CountingConnectionPoolMonitor();
        SimpleHostConnectionPool<TestClient> pool = new SimpleHostConnectionPool<TestClient>(
                host, new TestConnectionFactory(config, monitor), monitor,
                config, new NoOpListener());

        try {
            pool.growConnections(2);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        } catch (ConnectionException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        }
        Assert.assertEquals(2, pool.getActiveConnectionCount());
        Assert.assertEquals(false, pool.isShutdown());
        Assert.assertEquals(2, pool.getIdleConnectionCount());

        pool.markAsDown(null);

        Assert.assertEquals(0, pool.getActiveConnectionCount());
        Assert.assertEquals(0, pool.getIdleConnectionCount());
        Assert.assertEquals(true, pool.isShutdown());

        try {
            pool.growConnections(1);
            Assert.fail();
        } catch (HostDownException e) {
        } catch (Exception e) {
            Assert.fail();
        }

        Assert.assertEquals(0, pool.getActiveConnectionCount());
        Assert.assertEquals(0, pool.getIdleConnectionCount());
        Assert.assertEquals(true, pool.isShutdown());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Assert.assertEquals(1, pool.getActiveConnectionCount());
        Assert.assertEquals(1, pool.getIdleConnectionCount());
        Assert.assertEquals(false, pool.isShutdown());

        try {
            pool.growConnections(2);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        } catch (ConnectionException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        }
        Assert.assertEquals(3, pool.getActiveConnectionCount());
        Assert.assertEquals(false, pool.isShutdown());
        Assert.assertEquals(3, pool.getIdleConnectionCount());
    }

    @Test
    public void testCloseOrReturnConnection() {
        Host host = new Host("127.0.0.1", TestHostType.GOOD_FAST.ordinal());

        ConnectionPoolConfiguration config = createConfig();
        CountingConnectionPoolMonitor monitor = new CountingConnectionPoolMonitor();
        SimpleHostConnectionPool<TestClient> pool = new SimpleHostConnectionPool<TestClient>(
                host, new TestConnectionFactory(config, monitor), monitor,
                config, new NoOpListener());

        try {
            pool.growConnections(2);

            Assert.assertEquals(2, pool.getActiveConnectionCount());
            Assert.assertEquals(false, pool.isShutdown());
            Assert.assertEquals(2, pool.getIdleConnectionCount());

            Connection<TestClient> connection = pool.borrowConnection(0);

            Assert.assertEquals(2, pool.getActiveConnectionCount());
            Assert.assertEquals(false, pool.isShutdown());
            Assert.assertEquals(1, pool.getIdleConnectionCount());

            pool.returnConnection(connection);

            Assert.assertEquals(2, pool.getActiveConnectionCount());
            Assert.assertEquals(false, pool.isShutdown());
            Assert.assertEquals(2, pool.getIdleConnectionCount());

            connection = pool.borrowConnection(0);

            Assert.assertEquals(2, pool.getActiveConnectionCount());
            Assert.assertEquals(false, pool.isShutdown());
            Assert.assertEquals(1, pool.getIdleConnectionCount());

        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        } catch (ConnectionException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testAsyncOpenConnection() {
        Host host = new Host("127.0.0.1", TestHostType.GOOD_FAST.ordinal());

        ConnectionPoolConfigurationImpl config = createConfig();
        config.setMaxConnsPerHost(1);

        // Open the first connection
        CountingConnectionPoolMonitor monitor = new CountingConnectionPoolMonitor();
        SimpleHostConnectionPool<TestClient> pool = new SimpleHostConnectionPool<TestClient>(
                host, new TestConnectionFactory(config, monitor), monitor,
                config, new NoOpListener());

        try {
            Connection<TestClient> connection = pool
                    .borrowConnection(WAIT_TIMEOUT);
        } catch (ConnectionException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        }
        Assert.assertEquals(1, pool.getActiveConnectionCount());
        Assert.assertEquals(false, pool.isShutdown());
        Assert.assertEquals(0, pool.getIdleConnectionCount());

        // Subsequent open should fail
        try {
            Connection<TestClient> connection = pool
                    .borrowConnection(WAIT_TIMEOUT);
            Assert.fail();
        } catch (PoolTimeoutException e) {
        } catch (ConnectionException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        }
        Assert.assertEquals(1, pool.getActiveConnectionCount());
        Assert.assertEquals(false, pool.isShutdown());
        Assert.assertEquals(0, pool.getIdleConnectionCount());
    }

    @Test
    @Ignore
    public void testAsyncOpenConnectionWithShutdown() {
        Host host = new Host("127.0.0.1", TestHostType.GOOD_SLOW.ordinal());

        ConnectionPoolConfigurationImpl config = createConfig();
        config.setMaxConnsPerHost(1);

        // Open the first connection
        CountingConnectionPoolMonitor monitor = new CountingConnectionPoolMonitor();
        SimpleHostConnectionPool<TestClient> pool = new SimpleHostConnectionPool<TestClient>(
                host, new TestConnectionFactory(config, monitor), monitor,
                config, new NoOpListener());

        try {
            Connection<TestClient> connection = pool.borrowConnection(1);
        } catch (PoolTimeoutException e) {
        } catch (ConnectionException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        }
        pool.markAsDown(null);
        Assert.assertEquals(1, pool.getActiveConnectionCount());
        Assert.assertEquals(true, pool.isShutdown());
        Assert.assertEquals(0, pool.getIdleConnectionCount());

        // This should fail because we shut down
        try {
            Connection<TestClient> connection = pool
                    .borrowConnection(WAIT_TIMEOUT);
            Assert.fail();
        } catch (HostDownException e) {
        } catch (ConnectionException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        }

        // Count should still be 1 because we have a pending connection
        Assert.assertEquals(1, pool.getActiveConnectionCount());
        Assert.assertEquals(true, pool.isShutdown());
        Assert.assertEquals(0, pool.getIdleConnectionCount());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }

        Assert.assertEquals(0, pool.getActiveConnectionCount());
        Assert.assertEquals(true, pool.isShutdown());
        Assert.assertEquals(0, pool.getIdleConnectionCount());
    }

    @Test
    public void testAsyncOpenConnectionWithCheckedException() {
        Host host = new Host("127.0.0.1",
                TestHostType.CONNECT_TIMEOUT.ordinal());

        ConnectionPoolConfigurationImpl config = createConfig();

        // Open the first connection
        CountingConnectionPoolMonitor monitor = new CountingConnectionPoolMonitor();
        SimpleHostConnectionPool<TestClient> pool = new SimpleHostConnectionPool<TestClient>(
                host, new TestConnectionFactory(config, monitor), monitor,
                config, new NoOpListener());

        try {
            Connection<TestClient> connection = pool
                    .borrowConnection(WAIT_TIMEOUT);
            Assert.fail();
        } catch (ConnectionException e) {
        }
        Assert.assertEquals(0, pool.getActiveConnectionCount());
        Assert.assertEquals(true, pool.isShutdown());
        Assert.assertEquals(0, pool.getIdleConnectionCount());
    }

    @Test
    public void testAsyncOpenConnectionWithUnCheckedException() {
        Host host = new Host("127.0.0.1",
                TestHostType.CONNECT_WITH_UNCHECKED_EXCEPTION.ordinal());

        ConnectionPoolConfigurationImpl config = createConfig();

        // Open the first connection
        CountingConnectionPoolMonitor monitor = new CountingConnectionPoolMonitor();
        SimpleHostConnectionPool<TestClient> pool = new SimpleHostConnectionPool<TestClient>(
                host, new TestConnectionFactory(config, monitor), monitor,
                config, new NoOpListener());

        try {
            Connection<TestClient> connection = pool
                    .borrowConnection(WAIT_TIMEOUT);
            Assert.fail();
        } catch (ConnectionException e) {
        }
        Assert.assertEquals(0, pool.getActiveConnectionCount());
        Assert.assertEquals(true, pool.isShutdown());
        Assert.assertEquals(0, pool.getIdleConnectionCount());
    }

    public ConnectionPoolConfigurationImpl createConfig() {
        ConnectionPoolConfigurationImpl config = new ConnectionPoolConfigurationImpl(
                "cluster_keyspace");
        config.setMaxConnsPerHost(2);
        config.setInitConnsPerHost(1);
        return config;
    }

}
