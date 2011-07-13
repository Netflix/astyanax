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
