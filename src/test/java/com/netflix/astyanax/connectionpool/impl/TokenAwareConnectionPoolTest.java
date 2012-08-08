package com.netflix.astyanax.connectionpool.impl;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.dht.BigIntegerToken;
import org.apache.cassandra.dht.Token;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.retry.RunOnce;
import com.netflix.astyanax.test.TestClient;
import com.netflix.astyanax.test.TestConnectionFactory;
import com.netflix.astyanax.test.TestConstants;
import com.netflix.astyanax.test.TestHostType;
import com.netflix.astyanax.test.TokenTestOperation;
import com.netflix.astyanax.util.TokenGenerator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TokenAwareConnectionPoolTest extends BaseConnectionPoolTest {
    private static Logger LOG = LoggerFactory
            .getLogger(TokenAwareConnectionPoolTest.class);

    protected ConnectionPool<TestClient> createPool() {
        ConnectionPoolConfiguration config = new ConnectionPoolConfigurationImpl(
                TestConstants.CLUSTER_NAME + "_" + TestConstants.KEYSPACE_NAME);

        CountingConnectionPoolMonitor monitor = new CountingConnectionPoolMonitor();

        return new TokenAwareConnectionPoolImpl<TestClient>(
                config, new TestConnectionFactory(config, monitor), monitor);

    }


    @Test
    public void testTokenMappingForMidRangeTokens() throws ConnectionException {
        ConnectionPool<TestClient> cp = createPool();

        Map<Token, List<Host>> ring1 = makeRing(3, 1, 1);
        cp.setHosts(ring1);

        BigInteger threeNodeRingIncrement = TokenGenerator.MAXIMUM.divide(new BigInteger("3"));

        Operation<TestClient, String> firstHostOp = new TokenTestOperation(new BigIntegerToken(BigInteger.ZERO));
        Operation<TestClient, String> secondHostOp = new TokenTestOperation(new BigIntegerToken(BigInteger.ONE));
        Operation<TestClient, String> thirdHostOp = new TokenTestOperation(
                new BigIntegerToken(threeNodeRingIncrement.add(BigInteger.ONE)));

        RetryPolicy retryPolicy = new RunOnce();

        OperationResult<String> result = cp.executeWithFailover(firstHostOp, retryPolicy);
        assertNotNull(result);
        assertEquals("127.0.1.0",result.getHost().getIpAddress());

        result = cp.executeWithFailover(secondHostOp, retryPolicy);
        assertNotNull(result);
        assertEquals("127.0.1.1",result.getHost().getIpAddress());

        result = cp.executeWithFailover(thirdHostOp, retryPolicy);
        assertNotNull(result);
        assertEquals("127.0.1.2",result.getHost().getIpAddress());

    }

    @Test
    public void testTokenMappingForOrdinalTokens() throws ConnectionException {
        ConnectionPool<TestClient> cp = createPool();

        // the following will generate a ring of two nodes with the following characteristics;
        //    node1 - ip = 127.0.1.0, token ownership range = (1200 , 0]
        //    node1 - ip = 127.0.1.1, token ownership range = (0 , 600]
        //    node1 - ip = 127.0.1.1, token ownership range = (600 , 1200]
        Map<Token, List<Host>> ring1 = makeRing(3, 1, 1, BigInteger.ZERO, new BigInteger("1800"));
        cp.setHosts(ring1);

        BigInteger threeNodeRingIncrement = new BigInteger("600");

        Operation<TestClient, String> firstHostOp = new TokenTestOperation(new BigIntegerToken(BigInteger.ZERO));
        Operation<TestClient, String> secondHostOp = new TokenTestOperation(
                new BigIntegerToken(threeNodeRingIncrement));
        Operation<TestClient, String> thirdHostOp = new TokenTestOperation(
                new BigIntegerToken(threeNodeRingIncrement.multiply(new BigInteger("2"))));
        Operation<TestClient, String> maxTokenHostOp = new TokenTestOperation(
                new BigIntegerToken(threeNodeRingIncrement.multiply(new BigInteger("3"))));

        RetryPolicy retryPolicy = new RunOnce();

        OperationResult<String> result = cp.executeWithFailover(firstHostOp, retryPolicy);
        assertNotNull(result);
        assertEquals("127.0.1.0",result.getHost().getIpAddress());

        result = cp.executeWithFailover(secondHostOp, retryPolicy);
        assertNotNull(result);
        assertEquals("127.0.1.1",result.getHost().getIpAddress());

        result = cp.executeWithFailover(thirdHostOp, retryPolicy);
        assertNotNull(result);
        assertEquals("127.0.1.2",result.getHost().getIpAddress());

        result = cp.executeWithFailover(maxTokenHostOp, retryPolicy);
        assertNotNull(result);
        assertEquals("127.0.1.0",result.getHost().getIpAddress());

    }


    @Test
    public void testTokenToHostMappingInWrappedRange() throws ConnectionException {
        ConnectionPool<TestClient> cp = createPool();


        // the following will generate a ring of two nodes with the following characteristics;
        //    node1 - ip = 127.0.1.0, token ownership range = (510 , 10]
        //    node1 - ip = 127.0.1.1, token ownership range = (10 , 510]
        Map<Token, List<Host>> ring1 = makeRing(2, 1, 1, BigInteger.TEN, new BigInteger("1010"));
        cp.setHosts(ring1);

        Operation<TestClient, String> op = new TokenTestOperation(new BigIntegerToken(BigInteger.ZERO));

        RetryPolicy retryPolicy = new RunOnce();

        OperationResult<String> result = cp.executeWithFailover(op, retryPolicy);
        assertNotNull(result);

        // since token ownership wraps node2 should own token 0
        assertEquals("127.0.1.0",result.getHost().getIpAddress());
    }


     @Test
    public void testTokenToHostMappingOutsideOfRing() throws ConnectionException {
        ConnectionPool<TestClient> cp = createPool();

        // the following will generate a ring of two nodes with the following characteristics;
        //    node1 - ip = 127.0.1.0, token ownership range = (500 , 0]
        //    node1 - ip = 127.0.1.1, token ownership range = (0 , 500]
        Map<Token, List<Host>> ring1 = makeRing(2, 1, 1, BigInteger.ZERO, new BigInteger("1000"));
        cp.setHosts(ring1);

        Operation<TestClient, String> op = new TokenTestOperation(new BigIntegerToken("1250"));

        RetryPolicy retryPolicy = new RunOnce();

        OperationResult<String> result = cp.executeWithFailover(op, retryPolicy);
        assertNotNull(result);

        // requests for tokens outside the ring will be serviced by host associated with
        // 1st partition in ring
        assertEquals("127.0.1.0",result.getHost().getIpAddress());
    }



    @Test
    public void changeRingTest() {
        ConnectionPool<TestClient> cp = createPool();
        Map<Token, List<Host>> ring1 = makeRing(6, 3, 1);
        Map<Token, List<Host>> ring2 = makeRing(6, 3, 2);

        cp.setHosts(ring1);
        List<HostConnectionPool<TestClient>> hosts1 = cp.getActivePools();

        cp.setHosts(ring2);
        List<HostConnectionPool<TestClient>> hosts2 = cp.getActivePools();

        System.out.println(hosts1);
        System.out.println(hosts2);
    }

    private Map<Token, List<Host>> makeRing(int nHosts,
            int replication_factor, int id) {
        return makeRing(nHosts,replication_factor,id,TokenGenerator.MINIMUM,TokenGenerator.MAXIMUM);
    }

    private Map<Token, List<Host>> makeRing(int nHosts,
            int replication_factor, int id, BigInteger minInitialToken, BigInteger maxInitialToken) {
        List<Host> hosts = Lists.newArrayList();
        for (int i = 0; i < nHosts; i++) {
            hosts.add(new Host("127.0." + id + "." + i + ":"
                    + TestHostType.GOOD_FAST.ordinal(), 7160));
        }

        Map<Token, List<Host>> ring = Maps.newHashMap();
        for (int i = 0; i < nHosts; i++) {
            String token = TokenGenerator.initialToken(nHosts, i, minInitialToken, maxInitialToken);
            System.out.println(token);
            List<Host> replicas = new ArrayList<Host>();
            for (int j = 0; j < replication_factor; j++) {
                Host host = hosts.get((i + j) % nHosts);
                System.out.println(" " + host);
                replicas.add(host);
            }
            ring.put(new BigIntegerToken(token), replicas);
        }

        return ring;
    }
}
