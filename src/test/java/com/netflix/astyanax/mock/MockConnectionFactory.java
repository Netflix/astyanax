package com.netflix.astyanax.mock;

import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.ConnectionFactory;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;

public class MockConnectionFactory implements ConnectionFactory<MockClient> {
	@Override
	public Connection<MockClient> createConnection(
			final HostConnectionPool<MockClient> pool)
			throws ConnectionException, OperationException {
		return new Connection<MockClient>() {
			private boolean closed = false;
			private ConnectionException lastException;
			@Override
			public <R> OperationResult<R> execute(
					Operation<MockClient, R> op)
					throws ConnectionException {
				try {
					lastException = null;
					MockHostType type = MockHostType.get(pool.getHost().getPort());
					return type.execute(pool, op);
				}
				catch (ConnectionException e) {
					close();
					lastException = e;
					throw e;
				}
			}



			@Override
			public void close() {
				closed = true;
			}

			@Override
			public boolean isOpen() {
				return !closed;
			}

			@Override
			public HostConnectionPool<MockClient> getHostConnectionPool() {
				return pool;
			}

			@Override
			public ConnectionException getLastException() {
				return lastException;
			}

			@Override
			public void open()
					throws ConnectionException,
					BadRequestException {
				MockHostType type = MockHostType.get(pool.getHost().getPort());
				type.open(0);
			}
			
		};
	}		
}