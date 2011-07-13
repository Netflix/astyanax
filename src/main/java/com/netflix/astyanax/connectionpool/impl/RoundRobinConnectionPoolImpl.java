package com.netflix.astyanax.connectionpool.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.ConnectionFactory;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NoAvailableHostsException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.exceptions.PoolTimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.TimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.TransportException;
import com.netflix.astyanax.connectionpool.exceptions.UnknownException;

public class RoundRobinConnectionPoolImpl<CL> extends AbstractHostPartitionConnectionPool<CL> {

	private final AtomicInteger current = new AtomicInteger(0);
	private final AtomicReference<List<HostConnectionPool<CL>>> pools = new
		AtomicReference<List<HostConnectionPool<CL>>>();
	
	public RoundRobinConnectionPoolImpl(ConnectionPoolConfiguration config, 
			ConnectionFactory<CL> factory) {
		super(config, factory);
	}
	
	@Override
	public <R> Connection<CL> borrowConnection(Operation<CL, R> op)
			throws ConnectionException, OperationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R> OperationResult<R> executeWithFailover(Operation<CL, R> op)
			throws ConnectionException, OperationException {
		List<HostConnectionPool<CL>> pools = this.pools.get();
		if (pools.isEmpty()) {
			throw new NoAvailableHostsException("No hosts to borrow from");
		}
		
		int size = pools.size();
		int failoverCount = this.failoverStrategy.getMaxRetries();
		if (failoverCount < 0) 
			failoverCount = size;
		
		int retryCount = this.exhaustedStrategy.getMaxRetries();
		if (retryCount < 0) {
			retryCount = size;
		}
		
		int index = current.incrementAndGet()%size;
		do {
			// 1. Select the next pool
			HostConnectionPool<CL> pool = pools.get(index++);
			
			// 2. Try to get a connection
			Connection<CL> connection = null;
			try {
				long startTime = System.currentTimeMillis();
				connection = pool.borrowConnection(this.exhaustedStrategy.getWaitTime());
				this.monitor.incConnectionBorrowed(pool.getHost(), System.currentTimeMillis() - startTime);
			}
			catch (ConnectionException e) {
				// These are failures trying to open a new connection
				if (e instanceof TimeoutException || e instanceof TransportException || e instanceof UnknownException) {
					if (this.badHostDetector.checkFailure(pool.getHost(), e)) {
						this.markHostAsDown(pool, e);
					}
				}
				
				if (!e.isRetryable())
					throw e;
				
				failoverCount--;
				retryCount--;
				if (failoverCount > 0) {
					this.monitor.incFailover();
				}
				else if (retryCount <= 0) {
					throw new PoolTimeoutException("Timed out trying to borrow a connection");
				}
				
				this.monitor.incBorrowRetry();
				continue;
			}
			
			// 3. Now try to execute
			try {
				OperationResult<R> result = connection.execute(op);
				this.monitor.incOperationSuccess(pool.getHost(), result.getLatency());
				return result;
			}
			catch (ConnectionException e) {
				this.monitor.incOperationFailure(pool.getHost());
				if (!e.isRetryable()) {
					System.out.println("Not retryable");
					throw e;
				}
			}
			catch (Exception e) {
				this.monitor.incOperationFailure(pool.getHost());
				throw new RuntimeException(e);
			}
			finally {
				returnConnection(connection);
			}
			
			// 4. Apply the failover strategy
			if (--failoverCount > 0) {
				this.monitor.incFailover();
				if (this.failoverStrategy.getWaitTime() > 0) {
					try {
						Thread.sleep(this.failoverStrategy.getWaitTime());
					} 
					catch (InterruptedException e) {
						throw new TimeoutException("Interrupted sleeping between retries");
					}
				}
			}
			else {
				throw new TimeoutException("Operation failed too many times");
			}
		} while(true);
	}
	
	@Override 
	protected void onHostDown(HostConnectionPool<CL> pool) {
		rebuildPools();
	}
	
	@Override 
	protected void onHostUp(HostConnectionPool<CL> pool) {
		rebuildPools();
	}

	private void rebuildPools() {
		Collection<HostConnectionPool<CL>> hosts = activeHosts.values();
		if (hosts != null && !hosts.isEmpty()) {
			this.pools.set(new ArrayList<HostConnectionPool<CL>>(hosts));
		}
	}
}
