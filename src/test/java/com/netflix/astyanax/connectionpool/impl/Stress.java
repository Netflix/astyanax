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

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NoAvailableHostsException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.RoundRobinConnectionPoolImpl;
import com.netflix.astyanax.fake.TestClient;
import com.netflix.astyanax.fake.TestConnectionFactory;
import com.netflix.astyanax.fake.TestConstants;
import com.netflix.astyanax.fake.TestHostType;
import com.netflix.astyanax.fake.TestOperation;
import com.netflix.astyanax.retry.RunOnce;

public class Stress {
	private static Logger  LOG = LoggerFactory.getLogger(Stress.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		final int numThreads = 100;
		final int numHosts = 15;
		final int numOperations = 1000000000;
		
		ConnectionPoolConfigurationImpl config;
    	config = new ConnectionPoolConfigurationImpl(TestConstants.CLUSTER_NAME + "_" + TestConstants.KEYSPACE_NAME);
    	config.setMaxConns(100);
    	config.setMaxFailoverCount(-1);
    	config.setMaxTimeoutWhenExhausted(1000);
    	config.setMaxConnsPerHost(10);
    	config.setInitConnsPerHost(0);
    	// config.setRetryBackoffStrategy(new ExponentialRetryBackoffStrategy(20, 1000, 2000));

    	ConnectionPoolMonitor monitor = new Slf4jConnectionPoolMonitorImpl();
		
    	TestConnectionFactory factory = new TestConnectionFactory(config, monitor);
    	final ConnectionPool<TestClient> pool = new RoundRobinConnectionPoolImpl<TestClient>(config, factory, monitor);
    	for (int i = 0; i < numHosts; i++) {
    	    pool.addHost(new Host("127.0.0." + i, TestHostType.GOOD_FAST.ordinal()), true);
    	    //pool.addHost(new Host("127.0." + i + ".0", MockHostType.FAIL_AFTER_100_RANDOM.ordinal()));
            //pool.addHost(new Host("127.0." + i + ".0", MockHostType.FAIL_AFTER_100.ordinal()));
            // pool.addHost(new Host("127.0.0" + i, MockHostType.GOOD_FAST.ordinal()));
    	}
        pool.addHost(new Host("127.0.0." + numHosts, TestHostType.SOCKET_TIMEOUT_AFTER10.ordinal()), true);
    	
        //pool.addHost(new Host("127.0." + numHosts + ".0", MockHostType.FAIL_AFTER_10_SLOW_CLOSE.ordinal()));
        
		LOG.info(monitor.toString());
		
		long startTime = System.currentTimeMillis();
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		for (int i = 0; i < numThreads; i++) {
			executor.submit(new Runnable() {
				@Override
				public void run() {
					for (int i = 0; i < numOperations; i++) {
						long startTime = System.currentTimeMillis();
						try {
							OperationResult<String> result = pool.executeWithFailover(
								new TestOperation() {
									@Override
									public String execute(TestClient client)
											throws ConnectionException, OperationException {
										
										/*
									    long tm1 = System.currentTimeMillis();
									    think(5, 10);
									    try {
    							            double p = new Random().nextDouble();
    							            double factor = 50000;
    							            if (p < 1/factor) {
    							                throw new UnknownException("UnknownException");
    							            }
    							            else if (p < 2/factor) {
    							                throw new BadRequestException("BadRequestException");
    							            }
    							            else if (p < 3/factor) {
    							                think(1000, 0);
    							                throw new OperationTimeoutException("OperationTimeoutException");
    							            }
    							            else if (p < 4/factor) {
    							                throw new ConnectionAbortedException("ConnectionAbortedException");
    							            }
    							            else if (p < 5/factor) {
    							                throw new HostDownException("HostDownException");
    							            }
    							            else if (p < 6/factor) {
    							                think(1000, 0);
    							                throw new TimeoutException("TimeoutException");
    							            }
    							            else if (p < 7/factor) {
    							                throw new TokenRangeOfflineException("TokenRangeOfflineException");
    							            }
    							            else if (p < 8/factor) {
    							                throw new TransportException("TransportException");
    							            }
									    }
									    catch (ConnectionException e) {
									        e.setLatency(System.currentTimeMillis() - tm1);
									        throw e;
									    }
							           									// LOG.info("Execute 1");
							           									 
							           									 */
										return "RESULT";
									}
								}
							, RunOnce.get());
						} catch (NoAvailableHostsException e) {
						    //think(1000, 0);
						} catch (Exception e) {
                            LOG.error(e.getMessage());
						}
					}
				}
			});
		}
		
		try {
			executor.shutdown();
			int i = 0;
			while (!executor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
				LOG.info(monitor.toString());
				/*
				if (i++ % 10 == 9) {
			    	// config.setMaxConnsPerHost(config.getMaxConnsPerHost() - 1);
				}
				*/
			}
		} catch (InterruptedException e) {
			LOG.error(e.getMessage());
		}
		long runTime = System.currentTimeMillis() - startTime;
		
		double opsRate = (numThreads * numOperations) / runTime;
		
		pool.shutdown();
		think(1000, 1000);
		
		LOG.info("*** DONE ***");
		LOG.info(monitor.toString());
		// LOG.info(runTime + " ops/msec");
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
		}
    }
}
