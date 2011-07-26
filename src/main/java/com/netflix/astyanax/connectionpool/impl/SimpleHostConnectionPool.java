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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.ConnectionFactory;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.MaxConnsPerHostReachedException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.exceptions.PoolTimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.TimeoutException;

/**
 * Pool of connections for a single host.  
 * 
 * @author elandau
 *
 */
public class SimpleHostConnectionPool<CL> implements HostConnectionPool<CL> {
	private static ExecutorService executor = Executors.newSingleThreadExecutor();
	
	/**
	 * List of available connections.  This list does not include connections
	 * that are currently borrowed.
	 */
	private final BlockingQueue<Connection<CL>> availableConnections;
	
	/**
	 * Number of active connections.  We track this separate from activeConnections.size()
	 * because of the performance hit on getting the size of a NonBlockingHashSet
	 * and to avoid some concurrency edge cases.
	 */
	private final AtomicInteger activeCount = new AtomicInteger(0);
	
	/**
	 * Maximum number of connections that may be allocated on this host
	 */
	private final int maxConnections;
	
	/**
	 * Factory used to create new connections when growing the pool
	 */
	private final ConnectionFactory<CL> factory;
	
	private final ConnectionPoolMonitor monitor;
	
	/**
	 * Host description associated with this pool
	 */
	private final Host host;
	
	public SimpleHostConnectionPool(Host host, ConnectionFactory<CL> factory, ConnectionPoolMonitor monitor, int maxConnections) {
		this.host = host;
		this.maxConnections = maxConnections;
		this.factory = factory;
		this.monitor = monitor;
		this.availableConnections = new LinkedBlockingQueue<Connection<CL>>();
	}

	/**
	 * Create a connection as long the max hasn't been reached
	 * 
	 * @param timeout - Max wait timeout if max connections have been allocated
	 * 					and pool is empty.  0 to throw a MaxConnsPerHostReachedException.
	 * @return
	 * @throws TimeoutException if timeout specified and no new connection is available
	 * 		   MaxConnsPerHostReachedException if max connections created and no
	 * 			timeout was specified
	 */
	@Override
	public Connection<CL> borrowConnection(int timeout) throws ConnectionException, OperationException {
		// 1.  Return any free connections
		Connection<CL> connection = this.availableConnections.poll();
		if (connection != null) {
			return connection;
		}
		
		// 2.  No available connections, make sure we don't exceed the per host 
		//	   open connection limit
		if (this.activeCount.get() == this.maxConnections) {
			// 3.  Wait for a connection to free up
			return waitForConnection(timeout);
		}
		else {
			// 4.  Make sure we're not over allocating
			if (this.activeCount.incrementAndGet() > this.maxConnections) {
				this.activeCount.decrementAndGet();
				return waitForConnection(timeout);
			}
			else {
				// 5.  Try to open a new connection
				try {
					connection = this.factory.createConnection(this);
					connection.open();
					this.monitor.incConnectionCreated(host);
					return connection;
				}
				catch (ConnectionException e) {
					this.monitor.incConnectionCreateFailed(host, e);
					this.activeCount.getAndDecrement();
					if (connection != null) {
						connection.close();
					}
					throw e;
				}
			}
		}
	}
	
	/**
	 * Internal method to wait for a connection from the available connection 
	 * pool.
	 * 
	 * @param timeout
	 * @return
	 * @throws ConnectionException
	 */
	private Connection<CL> waitForConnection(int timeout) throws ConnectionException {
		if (timeout > 0) {
			Connection<CL> connection = null;
			try {
				connection = this.availableConnections.poll(timeout, TimeUnit.MILLISECONDS);
				if (connection != null)
					return connection;
			} catch (InterruptedException e) {
	            Thread.currentThread().interrupt();
			} 
			throw new PoolTimeoutException(getHost().getName());
		}
		else {
			throw new MaxConnsPerHostReachedException(getHost().getName());
		}
	}
	
	/**
	 * Return a connection to this host
	 * @param connection
	 */
	@Override
	public void returnConnection(Connection<CL> connection) {
		if (connection.isOpen()) {
			this.availableConnections.add(connection);
		}
		else {
			this.activeCount.getAndDecrement();
		}
	}
	
	/**
	 * Get number of active connections
	 * @return
	 */
	@Override
	public int getActiveConnectionCount() {
		return this.activeCount.get();
	}

	@Override
	public int getIdleConnectionCount() {
		return this.availableConnections.size();
	}
	
	/**
	 * Mark the host as down.  No new connections will be created from this host.
	 * Connections currently in use will be allowed to continue processing.
	 */
	@Override
	public void shutdown() {
		executor.submit(new Runnable() {
			@Override
			public void run() {
				// Loop as long as we have active connections.  We are essentially
				// waiting for all connections 
				while (activeCount.get() > 0) {
					try {
						Connection<CL> connection = availableConnections.take();
						activeCount.decrementAndGet();
						connection.close();
						
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
			}
		});
	}

	@Override
	public Host getHost() {
		return this.host;
	}
	
	public String toString() {
		int idle = this.getIdleConnectionCount();
		int open = this.getActiveConnectionCount();
		return new StringBuilder()
			.append("SimpleHostConnectionPool[")
			.append("host=").append(this.host)
			.append(",open=").append(open)
			.append(",busy=").append(open - idle)
			.append(",idle=").append(idle)
			.append("]")
			.toString();
	}
}
