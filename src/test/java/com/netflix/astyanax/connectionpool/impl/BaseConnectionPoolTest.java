/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.connectionpool.impl;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NoAvailableHostsException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.exceptions.PoolTimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.TransportException;
import com.netflix.astyanax.retry.ConstantBackoff;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.retry.RunOnce;
import com.netflix.astyanax.test.TestClient;
import com.netflix.astyanax.test.TestConnectionFactory;
import com.netflix.astyanax.test.TestConstants;
import com.netflix.astyanax.test.TestHostType;
import com.netflix.astyanax.test.TestOperation;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import com.netflix.astyanax.connectionpool.ConnectionContext;

@Ignore
public abstract class BaseConnectionPoolTest {
    private static Logger LOG = Logger
            .getLogger(RoundRobinConnectionPoolImplTest.class);

    private static Operation<TestClient, String> dummyOperation = new TestOperation();

    // private static ConnectionPoolConfigurationImpl config;

    // @BeforeClass
    public static void setup() {
        // config = new
        // ConnectionPoolConfigurationImpl(MockConstants.CLUSTER_NAME,
        // MockConstants.KEYSPACE_NAME);
        // config.setConnectionPoolFactory(ConnectionPoolType.ROUND_ROBIN);
        // config.setMaxTimeoutWhenExhausted(0);
        // config.setMaxFailoverCount(-1);
    }

    protected abstract ConnectionPool<TestClient> createPool();

    @Test
    public void testAll() {
        ConnectionPool<TestClient> pool = createPool();

        for (int i = 0; i < 5; i++) {
            pool.addHost(
                    new Host("127.0." + i + ".0", TestHostType.GOOD_FAST
                            .ordinal()), true);
            // pool.addHost(new Host("127.0." + i + ".1",
            // MockHostType.LOST_CONNECTION.ordinal()));
            // pool.addHost(new Host("127.0." + i + ".1",
            // MockHostType.CONNECT_TIMEOUT.ordinal()));
            // pool.addHost(new Host("127.0." + i + ".1",
            // MockHostType.ALWAYS_DOWN.ordinal()));
            // pool.addHost(new Host("127.0." + i + ".1",
            // MockHostType.THRASHING_TIMEOUT.ordinal()));
            // pool.addHost(new Host("127.0." + i + ".1",
            // MockHostType.CONNECT_BAD_REQUEST_EXCEPTION.ordinal()));
        }

        for (int i = 0; i < 10; i++) {
            try {
                OperationResult<String> result = pool.executeWithFailover(
                        dummyOperation, RunOnce.get());
                LOG.info(result.getHost());
            } catch (OperationException e) {
                LOG.info(e.getMessage());
                Assert.fail(e.getMessage());
            } catch (ConnectionException e) {
                LOG.info(e.getCause());
                Assert.fail(e.getMessage());
            }
        }
    }

    @Test
    public void testRollingRestart() {
        ConnectionPool<TestClient> pool = createPool();

        List<Host> hosts = new ArrayList<Host>();
        for (int i = 0; i < 5; i++) {
            Host host = new Host("127.0." + i + ".0",
                    TestHostType.GOOD_FAST.ordinal());
            pool.addHost(host, true);
            hosts.add(host);
        }

        for (int i = 0; i < 5; i++) {
            try {
                OperationResult<String> result = pool.executeWithFailover(
                        new TestOperation() {
                            @Override
                            public String execute(TestClient client, ConnectionContext context)
                                    throws ConnectionException,
                                    OperationException {
                                throw new TransportException("He's dead jim");
                            }
                        }, RunOnce.get());
                Assert.fail();
            } catch (Exception e) {
            }
        }

    }

    @Test
    public void testAlwaysDown() {
        ConnectionPool<TestClient> pool = createPool();

        pool.addHost(new Host("127.0.0.1", TestHostType.ALWAYS_DOWN.ordinal()),
                true);

        try {
            pool.executeWithFailover(dummyOperation, RunOnce.get());
            Assert.fail();
        } catch (OperationException e) {
            LOG.info(e.getMessage());
        } catch (ConnectionException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testConnectTimeout() {
        ConnectionPool<TestClient> pool = createPool();

        pool.addHost(
                new Host("127.0.0.1", TestHostType.CONNECT_TIMEOUT.ordinal()),
                true);

        try {
            pool.executeWithFailover(dummyOperation, RunOnce.get());
            Assert.fail();
        } catch (OperationException e) {
            LOG.info(e.getMessage());
        } catch (ConnectionException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testOperationTimeoutTimeout() {
        ConnectionPool<TestClient> pool = createPool();

        pool.addHost(
                new Host("127.0.0.1", TestHostType.OPERATION_TIMEOUT.ordinal()),
                true);

        try {
            pool.executeWithFailover(dummyOperation, RunOnce.get());
            Assert.fail();
        } catch (OperationException e) {
            LOG.info(e.getMessage());
        } catch (ConnectionException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testTimeoutTimeout() {
        ConnectionPool<TestClient> pool = createPool();

        pool.addHost(
                new Host("127.0.0.1", TestHostType.SOCKET_TIMEOUT.ordinal()),
                true);

        try {
            pool.executeWithFailover(dummyOperation, RunOnce.get());
            Assert.fail();
        } catch (OperationException e) {
            LOG.info(e.getMessage());
        } catch (ConnectionException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testConnectBadRequest() {
        ConnectionPool<TestClient> pool = createPool();

        pool.addHost(new Host("127.0.0.1",
                TestHostType.CONNECT_BAD_REQUEST_EXCEPTION.ordinal()), true);

        try {
            pool.executeWithFailover(dummyOperation, RunOnce.get());
            Assert.fail();
        } catch (OperationException e) {
            LOG.info(e.getMessage());
        } catch (ConnectionException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testThrashingTimeout() {
        ConnectionPool<TestClient> pool = createPool();

        pool.addHost(
                new Host("127.0.0.1", TestHostType.THRASHING_TIMEOUT.ordinal()),
                true);

        for (int i = 0; i < 10; i++) {
            try {
                think(1);
                pool.executeWithFailover(dummyOperation, RunOnce.get());
            } catch (OperationException e) {
                LOG.info(e.getMessage());
            } catch (ConnectionException e) {
                LOG.info(e.getMessage());
            }
        }

    }

    @Test
    public void testGoodFast() {
        ConnectionPool<TestClient> pool = createPool();

        pool.addHost(new Host("127.0.0.1", TestHostType.GOOD_SLOW.ordinal()),
                true);

        for (int i = 0; i < 10; i++) {
            try {
                pool.executeWithFailover(dummyOperation, RunOnce.get());
                LOG.info("Success");
            } catch (OperationException e) {
                LOG.info(e.getMessage());
            } catch (ConnectionException e) {
                LOG.info(e.getMessage());
            }
        }
    }

    @Test
    public void testDefaultConfig() {
        ConnectionPoolConfiguration config = new ConnectionPoolConfigurationImpl(
                TestConstants.CLUSTER_NAME + "_" + TestConstants.KEYSPACE_NAME);
        CountingConnectionPoolMonitor monitor = new CountingConnectionPoolMonitor();
        try {
            ConnectionPool<TestClient> pool = new RoundRobinConnectionPoolImpl<TestClient>(
                    config, new TestConnectionFactory(config, monitor), monitor);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRestartedCluster() {
        ConnectionPool<TestClient> pool = createPool();

        Host host1 = new Host("127.0.0.1", TestHostType.GOOD_FAST.ordinal());
        List<Host> ring1 = Lists.newArrayList(host1);

        Host host2 = new Host("127.0.0.2", TestHostType.GOOD_FAST.ordinal());
        List<Host> ring2 = Lists.newArrayList(host2);

        List<Host> ring3 = Lists.newArrayList();

        pool.setHosts(ring1);
        Assert.assertTrue (pool.hasHost (host1));
        Assert.assertTrue (pool.isHostUp(host1));
        
        Assert.assertFalse(pool.hasHost (host2));
        Assert.assertFalse(pool.isHostUp(host2));

        try {
            OperationResult<String> result = pool.executeWithFailover(
                    dummyOperation, RunOnce.get());
            Assert.assertEquals(host1, result.getHost());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        pool.setHosts(ring3);
        Assert.assertFalse(pool.hasHost(host1));
        Assert.assertFalse(pool.hasHost(host2));
        try {
            OperationResult<String> result = pool.executeWithFailover(
                    dummyOperation, RunOnce.get());
            result = pool.executeWithFailover(dummyOperation, RunOnce.get());
            Assert.fail();
        } catch (NoAvailableHostsException e) {

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        pool.setHosts(ring2);
        Assert.assertTrue(pool.hasHost(host2));
        Assert.assertTrue(pool.isHostUp(host2));
        Assert.assertFalse(pool.hasHost(host1));
        Assert.assertFalse(pool.isHostUp(host1));

        try {
            OperationResult<String> result = pool.executeWithFailover(
                    dummyOperation, RunOnce.get());
            Assert.assertEquals(host2, result.getHost());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

    }

    @Test
    @Ignore
    public void testAddHostThatIsDown() {
        /*
         * ConnectionPoolConfigurationImpl config = new
         * ConnectionPoolConfigurationImpl(MockConstants.CLUSTER_NAME,
         * MockConstants.KEYSPACE_NAME); config.setRetryBackoffStrategy(new
         * FixedRetryBackoffStrategy(100, 0));
         * 
         * ConnectionPool<MockClient> pool = new
         * RoundRobinConnectionPoolImpl<MockClient>(config, new
         * MockConnectionFactory(config));
         */
        ConnectionPool<TestClient> pool = createPool();
        
        Host host1 = new Host("127.0.0.1",
                TestHostType.CONNECT_FAIL_FIRST.ordinal());
        List<Host> ring1 = Lists.newArrayList(host1);

        OperationResult<String> result;

        pool.setHosts(ring1);
        Assert.assertTrue(pool.hasHost(host1));
        Assert.assertTrue(pool.isHostUp(host1));

        try {
            pool.executeWithFailover(dummyOperation, RunOnce.get());
            Assert.fail();
        } catch (PoolTimeoutException e) {
        } catch (ConnectionException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        think(500);

        Assert.assertTrue(pool.hasHost(host1));
        Assert.assertTrue(pool.isHostUp(host1));

        try {
            pool.executeWithFailover(dummyOperation, RunOnce.get());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    @Ignore
    public void testConnectionAborted() {
        ConnectionPool<TestClient> pool = createPool();

        Host host = new Host("127.0.0.1",
                TestHostType.ABORTED_CONNECTION.ordinal());
        pool.addHost(host, true);

        OperationResult<String> result;

        try {
            result = pool.executeWithFailover(dummyOperation, RunOnce.get());
            Assert.fail();
        } catch (ConnectionException e) {
        }
    }

    @Test
    public void testRetryEmptyPool() {
        ConnectionPool<TestClient> pool = createPool();

        RetryPolicy retry = new RunOnce();
        try {
            pool.executeWithFailover(dummyOperation, retry);
            Assert.fail();
        } catch (ConnectionException e) {
            Assert.assertEquals(1, retry.getAttemptCount());
            LOG.error(e);
        }

        retry = new ConstantBackoff(1, 10);
        try {
            pool.executeWithFailover(dummyOperation, retry);
            Assert.fail();
        } catch (ConnectionException e) {
            Assert.assertEquals(10, retry.getAttemptCount());
            LOG.info(e);
        }
    }

    protected void think(long timeout) {
        try {
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
        }
    }

}
