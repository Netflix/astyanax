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

import java.util.Comparator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.ConnectionFactory;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.exceptions.TimeoutException;

/**
 * Implementation of a connection pool using a single BlockingLinkedQueue.
 * 
 * This connection pool will borrow connections from the hosts but keep them
 * in its own queue which is essentially one big pool of connections.  
 * 
 * @author elandau
 *
 * @param <CL>
 */
public class BlockingQueueConnectionPoolImpl<CL> extends HostPartitionConnectionPoolImpl<CL> {
	
	/**
	 * Blocking queue of available connections.  This queue tracks connections for
	 * all hosts
	 */
	private LinkedBlockingQueue<Connection<CL>> availableConnections;
	
	public BlockingQueueConnectionPoolImpl(ConnectionPoolConfiguration config, ConnectionFactory<CL> factory) {
		super(config, factory);
		this.availableConnections = new LinkedBlockingQueue<Connection<CL>>();
		
		this.loadBalancingStrategy = new SortedLoadBalancingStrategy(config, 
				new Comparator<HostConnectionPool<?>>() {
					@Override
					public int compare(HostConnectionPool<?> o1,
							HostConnectionPool<?> o2) {
						return o1.getActiveConnectionCount() - o2.getActiveConnectionCount();
					}
			});
	}
	
	@Override
	public <R> Connection<CL> borrowConnection(Operation<CL, R> op) throws ConnectionException, OperationException {
		Connection<CL> conn = null;
		do {
			// 1.  Try to get the next connection in the pool, without blocking
			conn = availableConnections.poll();
			if (conn == null) {
				// 2.  No connections available so try to create one
				try {
					conn = super.borrowConnection(op);
				}
				finally {
				}
				
				// 3.  Still no connection so wait
				if (conn == null) {
					try {
						conn = availableConnections.poll(
								config.getMaxTimeoutWhenExhausted(), 
								TimeUnit.MILLISECONDS);
						if (conn == null) {
							this.monitor.incPoolExhaustedTimeout();
							throw new TimeoutException("MaxTimeoutWhenExhausted");
						}
					} 
					catch (InterruptedException e) {
						throw new TimeoutException("Thread interrupted", e);
					}
				}
			}
			
			// 4.  Make sure the connection wasn't closed
			if (!conn.isOpen()) {
				returnConnection(conn);
				conn = null;
				continue;
			}
			else {
				return conn;
			}
		} while (true);
	}

	@Override
	public void returnConnection(Connection<CL> connection) {
		// Still open so put back on the queue
		if (connection.isOpen()) {
			this.monitor.incConnectionReturned(connection.getHostConnectionPool().getHost());
			this.availableConnections.offer(connection);
		}
		// Connection may have been closed or host was shut down
		else {
			super.returnConnection(connection);
		}
	}
}
