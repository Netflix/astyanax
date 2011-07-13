package com.netflix.astyanax.connectionpool.impl;

import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration.Factory;
import com.netflix.astyanax.connectionpool.ExhaustedStrategy;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.mock.MockClient;
import com.netflix.astyanax.mock.MockConnectionFactory;
import com.netflix.astyanax.mock.MockConstants;
import com.netflix.astyanax.mock.MockHostType;
import com.netflix.astyanax.mock.MockOperation;
import com.netflix.cassandra.config.ConnectionPoolType;
import com.netflix.cassandra.config.FailoverStrategyType;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class RoundRobinConnectionPoolImplTest {
	private static Logger  LOG = Logger.getLogger(RoundRobinConnectionPoolImplTest.class);
	
	private static Operation<MockClient, String> dummyOperation = new MockOperation();
	
	private static ConnectionPoolConfigurationImpl config;
		
	@BeforeClass
	public static void setup() {
    	config = new ConnectionPoolConfigurationImpl(MockConstants.CLUSTER_NAME, MockConstants.KEYSPACE_NAME);
    	config.setConnectionPoolFactory(ConnectionPoolType.ROUND_ROBIN);
    	config.setFailoverStrategyFactory(FailoverStrategyType.ALL);
    	config.setExhaustedStrategyFactory(new Factory<ExhaustedStrategy>() {
			@Override
			public ExhaustedStrategy createInstance(
					ConnectionPoolConfiguration config) {
				return new ExhaustedStrategyImpl(-1, 0);
			}
    		
    	});
	}
	
	@Test
    public void testAll()  {
    	ConnectionPool<MockClient> pool = 
    		new RoundRobinConnectionPoolImpl<MockClient>(config, 
    			new MockConnectionFactory());

    	for (int i = 0; i < 5; i++) {
    		pool.addHost(new Host("127.0." + i + ".0", MockHostType.GOOD_FAST.ordinal()));
    		//pool.addHost(new Host("127.0." + i + ".1", MockHostType.LOST_CONNECTION.ordinal()));
    		//pool.addHost(new Host("127.0." + i + ".1", MockHostType.CONNECT_TIMEOUT.ordinal()));
    		//pool.addHost(new Host("127.0." + i + ".1", MockHostType.ALWAYS_DOWN.ordinal()));
    		//pool.addHost(new Host("127.0." + i + ".1", MockHostType.THRASHING_TIMEOUT.ordinal()));
    		//pool.addHost(new Host("127.0." + i + ".1", MockHostType.CONNECT_BAD_REQUEST_EXCEPTION.ordinal()));
    	}

    	for (int i = 0; i < 10; i++) {
			try {
				OperationResult<String> result = pool.executeWithFailover(dummyOperation);
				LOG.info(result.getHost());
			} catch (OperationException e) {
				LOG.info(e.getMessage());
				Assert.fail();
			} catch (ConnectionException e) {
				LOG.info(e.getMessage());
				Assert.fail();
			}
    	}
    }
	
	@Test
    public void testAlwaysDown()  {
    	ConnectionPool<MockClient> pool = 
    		new RoundRobinConnectionPoolImpl<MockClient>(config, 
    			new MockConnectionFactory());
    	
    	pool.addHost(new Host("127.0.0.1", MockHostType.ALWAYS_DOWN.ordinal()));
    	
		try {
			pool.executeWithFailover(dummyOperation);
			Assert.fail();
		} catch (OperationException e) {
			LOG.info(e.getMessage());
		} catch (ConnectionException e) {
			LOG.info(e.getMessage());
		}
    }
    
	@Test
    public void testConnectTimeout()  {
    	ConnectionPool<MockClient> pool = 
    		new RoundRobinConnectionPoolImpl<MockClient>(config, 
    			new MockConnectionFactory());
    	
    	pool.addHost(new Host("127.0.0.1", MockHostType.CONNECT_TIMEOUT.ordinal()));
    	
		try {
			pool.executeWithFailover(dummyOperation);
			Assert.fail();
		} catch (OperationException e) {
			LOG.info(e.getMessage());
		} catch (ConnectionException e) {
			LOG.info(e.getMessage());
		}
    }
	
	@Test
    public void testConnectBadRequest()  {
    	ConnectionPool<MockClient> pool = 
    		new RoundRobinConnectionPoolImpl<MockClient>(config, 
    			new MockConnectionFactory());
    	
    	pool.addHost(new Host("127.0.0.1", MockHostType.CONNECT_BAD_REQUEST_EXCEPTION.ordinal()));
    	
		try {
			pool.executeWithFailover(dummyOperation);
			Assert.fail();
		} catch (OperationException e) {
			LOG.info(e.getMessage());
		} catch (ConnectionException e) {
			LOG.info(e.getMessage());
		}
    }

    @Test
    public void testThrashingTimeout()  {
    	ConnectionPool<MockClient> pool = 
    		new RoundRobinConnectionPoolImpl<MockClient>(config, 
    			new MockConnectionFactory());
    	
    	pool.addHost(new Host("127.0.0.1", MockHostType.THRASHING_TIMEOUT.ordinal()));
    	
    	for (int i = 0; i < 100; i++) {
    		try {
				think(100);
				pool.executeWithFailover(dummyOperation);
			} catch (OperationException e) {
				LOG.info(e.getMessage());
			} catch (ConnectionException e) {
				LOG.info(e.getMessage());
			} 
		}
    }
    
    @Test
    public void testGoodFast()  {
    	ConnectionPool<MockClient> pool = 
    		new RoundRobinConnectionPoolImpl<MockClient>(config, 
    			new MockConnectionFactory());
    	
    	pool.addHost(new Host("127.0.0.1", MockHostType.GOOD_SLOW.ordinal()));
    	
    	for (int i = 0; i < 10; i++) {
    		try {
				pool.executeWithFailover(dummyOperation);
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
    	ConnectionPoolConfiguration config = 
    		new ConnectionPoolConfigurationImpl(MockConstants.CLUSTER_NAME, MockConstants.KEYSPACE_NAME);
    	
    	try {
	    	ConnectionPool<MockClient> pool = 
	    		new RoundRobinConnectionPoolImpl<MockClient>(config, 
	    			new MockConnectionFactory());
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    		Assert.fail();
    	}
    }
    
    private void think(long timeout) {
    	try {
			Thread.sleep(timeout);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
}
