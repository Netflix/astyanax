package com.netflix.astyanax.connectionpool.impl;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
import com.netflix.astyanax.fake.TestClient;
import com.netflix.astyanax.fake.TestConnectionFactory;
import com.netflix.astyanax.fake.TestConstants;
import com.netflix.astyanax.fake.TestHostType;
import com.netflix.astyanax.fake.TestOperation;
import com.netflix.astyanax.util.TokenGenerator;

public class TokenAwareConnectionPoolTest extends BaseConnectionPoolTest {
	private static Logger  LOG = LoggerFactory.getLogger(TokenAwareConnectionPoolTest.class);
	
	private static Operation<TestClient, String> dummyOperation = new TestOperation();
	
	protected ConnectionPool<TestClient> createPool() {
    	ConnectionPoolConfiguration config = 
    		new ConnectionPoolConfigurationImpl(TestConstants.CLUSTER_NAME + "_" + TestConstants.KEYSPACE_NAME);
    	
		CountingConnectionPoolMonitor monitor = new CountingConnectionPoolMonitor();
		
    	ConnectionPool<TestClient> pool = 
    		new TokenAwareConnectionPoolImpl<TestClient>(config, 
    			new TestConnectionFactory(config, monitor), monitor);

    	return pool;
	}
	
	@Test
	public void changeRingTest() {
		ConnectionPool<TestClient> cp = createPool();
		Map<BigInteger, List<Host>> ring1 = makeRing(6, 3, 1);
		Map<BigInteger, List<Host>> ring2 = makeRing(6, 3, 2);
		
		cp.setHosts(ring1);
		List<HostConnectionPool<TestClient>> hosts1 = cp.getActivePools();
		
		cp.setHosts(ring2);
		List<HostConnectionPool<TestClient>> hosts2 = cp.getActivePools();
		
		System.out.println(hosts1);
		System.out.println(hosts2);
	}
	
	
	private Map<BigInteger, List<Host>> makeRing(int nHosts, int replication_factor, int id) {
		List<Host> hosts = Lists.newArrayList();
		for (int i = 0; i < nHosts; i++) {
			hosts.add(new Host("127.0." + id + "." + i + ":" + TestHostType.GOOD_FAST.ordinal(), 7160));
		}
		
		Map<BigInteger, List<Host>> ring = Maps.newHashMap();
		for (int i = 0; i < nHosts; i++) {
			String token = TokenGenerator.intialToken(nHosts, i);
			System.out.println(token);
			List<Host> replicas = new ArrayList<Host>();
			for (int j = 0; j < replication_factor; j++) {
				Host host = hosts.get((i + j) % nHosts);
				System.out.println(" " + host);
				replicas.add(host);
			}
			ring.put(new BigInteger(token), replicas);
		}
		
		return ring;
	}
}
