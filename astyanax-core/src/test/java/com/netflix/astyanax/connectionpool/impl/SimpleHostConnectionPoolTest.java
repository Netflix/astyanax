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
import com.netflix.astyanax.connectionpool.exceptions.TimeoutException;
import com.netflix.astyanax.shallows.EmptyPartitioner;
import com.netflix.astyanax.test.TestClient;
import com.netflix.astyanax.test.TestConnectionFactory;
import com.netflix.astyanax.test.TestHostType;
import com.netflix.astyanax.test.TestOperation;
import com.netflix.astyanax.connectionpool.ConnectionContext;

public class SimpleHostConnectionPoolTest {
    private static Logger LOG = LoggerFactory
            .getLogger(SimpleHostConnectionPoolTest.class);

    private static int WAIT_TIMEOUT = 50;

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
        Assert.assertEquals(0,      pool.getActiveConnectionCount());
        Assert.assertEquals(false,  pool.isReconnecting());
        Assert.assertEquals(0,      pool.getIdleConnectionCount());

        try {
            pool.primeConnections(1);
        } catch (ConnectionException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        }
        Assert.assertEquals(1,      pool.getActiveConnectionCount());
        Assert.assertEquals(false,  pool.isReconnecting());
        Assert.assertEquals(1,      pool.getIdleConnectionCount());
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
            pool.primeConnections(1);
            Assert.fail();
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        } catch (ConnectionException e) {
        }
        Assert.assertEquals(0,    pool.getActiveConnectionCount());
        Assert.assertEquals(true, pool.isReconnecting());
        Assert.assertEquals(0,    pool.getIdleConnectionCount());
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
            pool.primeConnections(1);
            Assert.fail();
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        } catch (ConnectionException e) {
        }
        
        LOG.info(pool.toString());
        
        Assert.assertEquals(0,    pool.getActiveConnectionCount());
        Assert.assertEquals(0,    pool.getIdleConnectionCount());
        Assert.assertEquals(0,    pool.getOpenedConnectionCount());
        Assert.assertEquals(0,    pool.getClosedConnectionCount());
        Assert.assertEquals(2,    pool.getFailedOpenConnectionCount());
        Assert.assertEquals(0,    pool.getBusyConnectionCount());
        Assert.assertEquals(0,    pool.getPendingConnectionCount());
        Assert.assertEquals(true, pool.isReconnecting());
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
            pool.primeConnections(2);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        } catch (ConnectionException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        }
        Assert.assertEquals(2,     pool.getActiveConnectionCount());
        Assert.assertEquals(false, pool.isReconnecting());
        Assert.assertEquals(2,     pool.getIdleConnectionCount());

        try {
            pool.primeConnections(2);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            Assert.fail();
        }
        Assert.assertEquals(2,     pool.getActiveConnectionCount());
        Assert.assertEquals(false, pool.isReconnecting());
    }

    @Test
    public void testFailFirst() throws Exception {
        Host host = new Host("127.0.0.1", TestHostType.CONNECT_FAIL_FIRST_TWO.ordinal());

        ConnectionPoolConfigurationImpl config = createConfig();
        config.setRetryBackoffStrategy(new FixedRetryBackoffStrategy(100, 100));
        CountingConnectionPoolMonitor monitor = new CountingConnectionPoolMonitor();
        SimpleHostConnectionPool<TestClient> pool = new SimpleHostConnectionPool<TestClient>(
                host, new TestConnectionFactory(config, monitor), monitor,
                config, new NoOpListener());

        try {
            pool.primeConnections(2);
            Assert.fail();
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
        } catch (ConnectionException e) {
            LOG.error(e.getMessage());
        }
        
        Assert.assertEquals(0,    pool.getActiveConnectionCount());
        Assert.assertEquals(true, pool.isReconnecting());
        Assert.assertEquals(0,    pool.getIdleConnectionCount());

        Thread.sleep(1000);
        
        Assert.assertEquals(1,     pool.getActiveConnectionCount());
        Assert.assertEquals(false, pool.isReconnecting());
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
            pool.primeConnections(2);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        } catch (ConnectionException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        }
        Assert.assertEquals(2,     pool.getActiveConnectionCount());
        Assert.assertEquals(false, pool.isReconnecting());
        Assert.assertEquals(2,     pool.getIdleConnectionCount());

        pool.markAsDown(null);

        Assert.assertEquals(2,    pool.getActiveConnectionCount());
        Assert.assertEquals(2,    pool.getIdleConnectionCount());
        Assert.assertEquals(true, pool.isReconnecting());

        try {
            pool.primeConnections(1);
        } catch (HostDownException e) {
        } catch (Exception e) {
            Assert.fail();
        }

        Assert.assertEquals(2,    pool.getActiveConnectionCount());
        Assert.assertEquals(2,    pool.getIdleConnectionCount());
        Assert.assertEquals(true, pool.isReconnecting());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Assert.assertEquals(2,     pool.getActiveConnectionCount());
        Assert.assertEquals(2,     pool.getIdleConnectionCount());
        Assert.assertEquals(false, pool.isReconnecting());

        try {
            pool.primeConnections(2);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        } catch (ConnectionException e) {
            LOG.error(e.getMessage());
            Assert.fail();
        }
        Assert.assertEquals(3,     pool.getActiveConnectionCount());
        Assert.assertEquals(false, pool.isReconnecting());
        Assert.assertEquals(3,     pool.getIdleConnectionCount());
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
            pool.primeConnections(2);

            Assert.assertEquals(2,     pool.getActiveConnectionCount());
            Assert.assertEquals(false, pool.isReconnecting());
            Assert.assertEquals(2,     pool.getIdleConnectionCount());

            Connection<TestClient> connection = pool.borrowConnection(0);

            Assert.assertEquals(2,     pool.getActiveConnectionCount());
            Assert.assertEquals(false, pool.isReconnecting());
            Assert.assertEquals(1,     pool.getIdleConnectionCount());

            pool.returnConnection(connection);

            Assert.assertEquals(2,     pool.getActiveConnectionCount());
            Assert.assertEquals(false, pool.isReconnecting());
            Assert.assertEquals(2,     pool.getIdleConnectionCount());

            connection = pool.borrowConnection(0);

            Assert.assertEquals(2,     pool.getActiveConnectionCount());
            Assert.assertEquals(false, pool.isReconnecting());
            Assert.assertEquals(1,     pool.getIdleConnectionCount());

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
        Assert.assertEquals(1,     pool.getActiveConnectionCount());
        Assert.assertEquals(false, pool.isReconnecting());
        Assert.assertEquals(0,     pool.getIdleConnectionCount());

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
        Assert.assertEquals(1,    pool.getActiveConnectionCount());
        Assert.assertEquals(false, pool.isReconnecting());
        Assert.assertEquals(0,    pool.getIdleConnectionCount());
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
        Assert.assertEquals(1,    pool.getActiveConnectionCount());
        Assert.assertEquals(true, pool.isReconnecting());
        Assert.assertEquals(0,    pool.getIdleConnectionCount());

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
        Assert.assertEquals(1,    pool.getActiveConnectionCount());
        Assert.assertEquals(true, pool.isReconnecting());
        Assert.assertEquals(0,    pool.getIdleConnectionCount());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }

        Assert.assertEquals(0,    pool.getActiveConnectionCount());
        Assert.assertEquals(true, pool.isReconnecting());
        Assert.assertEquals(0,    pool.getIdleConnectionCount());
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
        
        LOG.info(pool.toString());
        Assert.assertEquals(0,    pool.getActiveConnectionCount());
        Assert.assertEquals(0,    pool.getIdleConnectionCount());
        Assert.assertEquals(0,    pool.getOpenedConnectionCount());
        Assert.assertEquals(0,    pool.getClosedConnectionCount());
        Assert.assertEquals(1,    pool.getFailedOpenConnectionCount());
        Assert.assertEquals(0,    pool.getBusyConnectionCount());
        Assert.assertEquals(0,    pool.getPendingConnectionCount());
        Assert.assertEquals(false, pool.isReconnecting());
    }

    @Test
    public void testAsyncOpenConnectionWithUnCheckedException() throws Exception {
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
        
        Assert.assertEquals(0,     pool.getActiveConnectionCount());
        Assert.assertEquals(false, pool.isReconnecting());
        Assert.assertEquals(0,     pool.getIdleConnectionCount());
    }
    
    @Test
    public void testExcessiveTimeouts() throws Exception {
        Host host = new Host("127.0.0.1",
                TestHostType.GOOD_FAST.ordinal());

        ConnectionPoolConfigurationImpl config = createConfig();
        config.setRetryBackoffStrategy(new FixedRetryBackoffStrategy(500, 500));

        // Open the first connection
        CountingConnectionPoolMonitor monitor = new CountingConnectionPoolMonitor();
        SimpleHostConnectionPool<TestClient> pool = new SimpleHostConnectionPool<TestClient>(
                host, new TestConnectionFactory(config, monitor), monitor,
                config, new NoOpListener());

        try {
            for (int i = 0; i < 3; i++) {
                Connection<TestClient> connection = pool.borrowConnection(WAIT_TIMEOUT);
                try {
                    Assert.assertEquals(1,     pool.getActiveConnectionCount());
                    Assert.assertEquals(false, pool.isReconnecting());
                    Assert.assertEquals(0,     pool.getIdleConnectionCount());
                    
                    connection.execute(new TestOperation() {
                        @Override
                        public String execute(TestClient client, ConnectionContext context) throws ConnectionException {
                            throw new TimeoutException("Test");
                        }
                    });
                    
                    Assert.fail();
                }
                catch (Throwable t) {
                }
                finally {
                    pool.returnConnection(connection);
                }
                Assert.assertEquals(i+1,     pool.getErrorsSinceLastSuccess());
                Assert.assertEquals(0,     pool.getActiveConnectionCount());
                Assert.assertEquals(false, pool.isReconnecting());
                Assert.assertEquals(0,     pool.getIdleConnectionCount());
            }
            
            Connection<TestClient> connection = pool.borrowConnection(WAIT_TIMEOUT);
            try {
                Assert.assertEquals(1,     pool.getActiveConnectionCount());
                Assert.assertEquals(false, pool.isReconnecting());
                Assert.assertEquals(0,     pool.getIdleConnectionCount());
                
                connection.execute(new TestOperation() {
                    @Override
                    public String execute(TestClient client, ConnectionContext context) throws ConnectionException {
                        throw new TimeoutException("Test");
                    }
                });
            }
            catch (Throwable t) {
            }
            finally {
                pool.returnConnection(connection);
            }
            Assert.assertEquals(0,     pool.getActiveConnectionCount());
            Assert.assertEquals(true,  pool.isReconnecting());
            Assert.assertEquals(0,     pool.getIdleConnectionCount());

            Thread.sleep(1000);
            
            connection = pool.borrowConnection(WAIT_TIMEOUT);
            pool.returnConnection(connection);
            
            Assert.assertEquals(1,      pool.getActiveConnectionCount());
            Assert.assertEquals(false,  pool.isReconnecting());
            Assert.assertEquals(1,      pool.getIdleConnectionCount());
            
        } catch (ConnectionException e) {
            LOG.error("Error", e);
            Assert.fail(e.getMessage());
        }
    }

    public ConnectionPoolConfigurationImpl createConfig() {
        ConnectionPoolConfigurationImpl config = new ConnectionPoolConfigurationImpl("cluster_keyspace");
        
        config.setMaxConnsPerHost(2);
        config.setInitConnsPerHost(1);
        config.setConnectTimeout(200);
        config.setPartitioner(new EmptyPartitioner());
        
        config.initialize();
        
        return config;
    }

}
