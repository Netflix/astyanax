package com.netflix.astyanax.mock;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.TimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.TransportException;
import com.netflix.astyanax.connectionpool.impl.OperationResultImpl;

public enum MockHostType {
	ALWAYS_DOWN {
		@Override
		public <R> OperationResult<R> execute(HostConnectionPool<MockClient> pool, Operation<MockClient, R> op)
				throws ConnectionException {
			throw new TransportException("TransportException");
		}

		@Override
		public void open(long timeout) throws ConnectionException {
			throw new TransportException("TransportException");
		}
	},
	
	LOST_CONNECTION {
		@Override
		public <R> OperationResult<R> execute(HostConnectionPool<MockClient> pool, Operation<MockClient, R> op)
				throws ConnectionException {
			throw new TransportException("TransportException");
		}

		@Override
		public void open(long timeout) throws ConnectionException {
		}
	},
	
	CONNECT_TIMEOUT {
		@Override
		public <R> OperationResult<R> execute(HostConnectionPool<MockClient> pool, Operation<MockClient, R> op)
				throws ConnectionException {
			throw new TransportException("TransportException");
		}

		@Override
		public void open(long timeout) throws ConnectionException {
			throw new TimeoutException("TimeoutException");
		}
	},
	
	CONNECT_BAD_REQUEST_EXCEPTION {
		@Override
		public <R> OperationResult<R> execute(HostConnectionPool<MockClient> pool, Operation<MockClient, R> op)
				throws ConnectionException {
			throw new TransportException("TransportException");
		}

		@Override
		public void open(long timeout) throws ConnectionException {
			throw new BadRequestException("BadRequestException");
		}
	},
	
	GOOD_SLOW {
		@Override
		public <R> OperationResult<R> execute(HostConnectionPool<MockClient> pool, Operation<MockClient, R> op)
				throws ConnectionException {
			return new OperationResultImpl<R>(pool.getHost(), op.execute(null), think(200));
		}

		@Override
		public void open(long timeout) throws ConnectionException {
		}
	},
	
	GOOD_FAST {

		@Override
		public <R> OperationResult<R> execute(
				HostConnectionPool<MockClient> pool,
				Operation<MockClient, R> op) throws ConnectionException {
			return new OperationResultImpl<R>(pool.getHost(), op.execute(null), think(5));
		}

		@Override
		public void open(long timeout) throws ConnectionException {
		}
	},
	
	GOOD_IMMEDIATE {

		@Override
		public <R> OperationResult<R> execute(
				HostConnectionPool<MockClient> pool,
				Operation<MockClient, R> op) throws ConnectionException {
			return new OperationResultImpl<R>(pool.getHost(), op.execute(null), 0);
		}

		@Override
		public void open(long timeout) throws ConnectionException {
		}
	},
	
	THRASHING_TIMEOUT {

		@Override
		public <R> OperationResult<R> execute(
				HostConnectionPool<MockClient> pool,
				Operation<MockClient, R> op) throws ConnectionException {
			think(50 + new Random().nextInt(1000));
			throw new TimeoutException("thrashing_timeout");
		}

		@Override
		public void open(long timeout) throws ConnectionException {
		}
	};
	
	private static final Map<Integer,MockHostType> lookup 
    	= new HashMap<Integer,MockHostType>();

	static {
    	for(MockHostType type : EnumSet.allOf(MockHostType.class))
    		lookup.put(type.ordinal(), type);
		}
	
	public static MockHostType get(int ordinal) {
		return lookup.get(ordinal);
	}

	public abstract <R> OperationResult<R> execute(
			HostConnectionPool<MockClient> pool, Operation<MockClient, R> op)
			throws ConnectionException;
	
	public abstract void open(long timeout) throws ConnectionException;
	
	private static int think(int time) {
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
		}
		return time;
	}

}
