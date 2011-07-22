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

import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;

public class ImmutableHostConnectionPool<CL> implements HostConnectionPool<CL> {

	private final HostConnectionPool<CL> pool;
	
	public ImmutableHostConnectionPool(HostConnectionPool<CL> pool) {
		this.pool = pool;
	}
	
	@Override
	public Connection<CL> borrowConnection(int timeout)
			throws ConnectionException, OperationException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void returnConnection(Connection<CL> connection) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getActiveConnectionCount() {
		return pool.getActiveConnectionCount();
	}

	@Override
	public void shutdown() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Host getHost() {
		return pool.getHost();
	}

	@Override
	public int getIdleConnectionCount() {
		return pool.getIdleConnectionCount();
	}
	
	public String toString() {
		return pool.toString();
	}
}
