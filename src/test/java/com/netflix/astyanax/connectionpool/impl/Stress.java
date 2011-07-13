package com.netflix.astyanax.connectionpool.impl;

import java.math.BigInteger;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ExhaustedStrategy;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration.Factory;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.exceptions.PoolTimeoutException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.mock.MockClient;
import com.netflix.astyanax.mock.MockConnectionFactory;
import com.netflix.astyanax.mock.MockConstants;
import com.netflix.astyanax.mock.MockEmbeddedCassandra;
import com.netflix.astyanax.mock.MockHostType;
import com.netflix.astyanax.mock.MockOperation;
import com.netflix.astyanax.thrift.ThriftKeyspaceImplTest;
import com.netflix.cassandra.NetflixConnectionPoolMonitor;
import com.netflix.cassandra.config.ConnectionPoolType;
import com.netflix.cassandra.config.FailoverStrategyType;

public class Stress {
    private static final Logger LOG = Logger.getLogger(Stress.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		final int numThreads = 1000;
		final int numHosts = 15;
		final int numOperations = 2000;
		
		ConnectionPoolConfigurationImpl config;
    	config = new ConnectionPoolConfigurationImpl(MockConstants.CLUSTER_NAME, MockConstants.KEYSPACE_NAME);
    	config.setConnectionPoolFactory(ConnectionPoolType.ROUND_ROBIN);
    	config.setFailoverStrategyFactory(FailoverStrategyType.ALL);
    	config.setExhaustedStrategyFactory(new Factory<ExhaustedStrategy>() {
			@Override
			public ExhaustedStrategy createInstance(
					ConnectionPoolConfiguration config) {
				return new ExhaustedStrategyImpl(0, 20000);
			}
    	});
    	NetflixConnectionPoolMonitor monitor = new NetflixConnectionPoolMonitor("Test", true);
    	config.setConnectionPoolMonitor(monitor);
		
    	final ConnectionPool<MockClient> pool = 
    		new RoundRobinConnectionPoolImpl<MockClient>(config, 
    			new MockConnectionFactory());
    	
    	for (int i = 0; i < numHosts; i++) {
    		pool.addHost(new Host("127.0." + i + ".0", MockHostType.GOOD_IMMEDIATE.ordinal()));
    	}
		LOG.info(monitor.toString());
    	
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		for (int i = 0; i < numThreads; i++) {
			executor.submit(new Runnable() {
				@Override
				public void run() {
					for (int i = 0; i < numOperations; i++) {
						long startTime = System.currentTimeMillis();
						try {
							OperationResult<String> result = pool.executeWithFailover(
								new Operation<MockClient, String>() {
									@Override
									public String execute(MockClient client)
											throws ConnectionException, OperationException {
										think(10, 20);
										return "RESULT";
									}
	
									@Override
									public BigInteger getKey() {
										return null;
									}
	
									@Override
									public String getKeyspace() {
										return null;
									}
								}
							);
						} catch (PoolTimeoutException e) {
							LOG.info("Timeout " + (System.currentTimeMillis() - startTime));
						} catch (OperationException e) {
							e.printStackTrace();
						} catch (ConnectionException e) {
							e.printStackTrace();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			});
		}
		
		try {
			executor.shutdown();
			while (!executor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
				LOG.info(monitor.toString());
			}
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		LOG.info("*** DONE ***");
		LOG.info(monitor.toString());
		pool.shutdown();
	}

    private static void think(int min, int max) {
    	try {
    		if (max > min) {
    			Thread.sleep(min + new Random().nextInt(max-min));
    		}
    		else {
    			Thread.sleep(min);
    		}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
