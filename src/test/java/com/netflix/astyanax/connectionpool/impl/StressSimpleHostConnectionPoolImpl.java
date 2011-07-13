package com.netflix.astyanax.connectionpool.impl;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.mock.MockClient;
import com.netflix.astyanax.mock.MockConnectionFactory;
import com.netflix.astyanax.mock.MockHostType;

public class StressSimpleHostConnectionPoolImpl {
    private static final Logger LOG = Logger.getLogger(Stress.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Host host = new Host("127.0.0.1", MockHostType.GOOD_IMMEDIATE.ordinal());
		final SimpleHostConnectionPool pool = new SimpleHostConnectionPool<MockClient>(
				host, new MockConnectionFactory(), 3);
		
		int numThreads = 100;
		final int numOperations = 100;
		final int timeout = 2000;
		
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		for (int i = 0; i < numThreads; i++) {
			executor.submit(new Runnable() {
				@Override
				public void run() {
					for (int i = 0; i < numOperations; i++) {
						Connection<MockClient> conn = null;
						try {
							conn = pool.borrowConnection(timeout);
							think(10, 10);
						} catch (OperationException e) {
							// e.printStackTrace();
						} catch (ConnectionException e) {
							// e.printStackTrace();
						}
						finally {
							conn.getHostConnectionPool().returnConnection(conn);
						}
					}
				}
			});
		}
		
		try {
			executor.shutdown();
			while (!executor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
				LOG.info(pool.toString());
			}
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		LOG.info("**** FINISHED ****");
		LOG.info(pool.toString());
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
