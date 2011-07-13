package com.netflix.astyanax.thrift;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.AsyncClient;
import org.apache.cassandra.thrift.Cassandra.AsyncClient.set_keyspace_call;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransport;

import com.netflix.astyanax.connectionpool.AsyncOperation;
import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.ConnectionFactory;
import com.netflix.astyanax.connectionpool.FutureOperationResult;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.TimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.TransportException;
import com.netflix.astyanax.connectionpool.impl.OperationResultImpl;

public class ThriftAsyncConnectionFactoryImpl implements ConnectionFactory<Cassandra.AsyncClient> {

	private static final String NAME_FORMAT = "ThriftAsyncConnection<%s-%d>";
	private final AtomicLong idCounter = new AtomicLong(0);
	private final String keyspaceName;
	private TAsyncClientManager acm;
	
	public ThriftAsyncConnectionFactoryImpl(String keyspaceName) throws IOException {
		this.keyspaceName = keyspaceName;
		this.acm = new TAsyncClientManager();
	}
	
	@Override
	public Connection<Cassandra.AsyncClient> createConnection(final HostConnectionPool<Cassandra.AsyncClient> pool)
			throws ConnectionException {
		return new Connection<Cassandra.AsyncClient>() {
			
			private final long id = idCounter.incrementAndGet();
			private TNonblockingSocket clientSock;
			private Cassandra.AsyncClient cassandraClient;
			private ConnectionException lastException = null;

			@Override
			public <R> OperationResult<R> execute(Operation<AsyncClient, R> op)
					throws ConnectionException {
				try {
					lastException = null;
					long latency = System.currentTimeMillis();
					R result = op.execute(this.cassandraClient);
					latency = System.currentTimeMillis() - latency;
					return new OperationResultImpl<R>(pool.getHost(), result, latency);
				}
				catch (TransportException e) {
					close();
					lastException = e;
					throw lastException;
				}
				catch (TimeoutException e) {
					close();
					lastException = e;
					throw lastException;
				}
				catch (ConnectionException e) {
					lastException = e;
					throw lastException;
				}				
			}

			@Override
			public void close() {
				clientSock.close();
			}

			@Override
			public boolean isOpen() {
				return clientSock.isOpen();
			}

			@Override
			public HostConnectionPool<AsyncClient> getHostConnectionPool() {
				return pool;
			}

			@Override
			public ConnectionException getLastException() {
				return this.lastException;
			}

			@Override
			public String toString() {
			    return String.format(NAME_FORMAT, pool.getHost().getHostName(), id);
			}

			@Override
			public void open() throws ConnectionException {
				try {
					clientSock = new TNonblockingSocket(
						      pool.getHost().getIpAddress(), pool.getHost().getPort());
					cassandraClient = 
						new Cassandra.AsyncClient(
							new TBinaryProtocol.Factory(), acm, clientSock);
					// cassandraClient.setTimeout(timeout);
					
					final CountDownLatch latch = new CountDownLatch(1);
					cassandraClient.set_keyspace(keyspaceName, new AsyncMethodCallback<set_keyspace_call>() {
						@Override
						public void onComplete(set_keyspace_call response) {
							latch.countDown();
						}

						@Override
						public void onError(Exception exception) {
							// TODO Auto-generated method stub
							
						}
					});
				} catch (IOException e) {
					throw new TransportException(e);
				} catch (TException e) {
					throw new TransportException(e);
				}
			}

			@Override
			public <R> FutureOperationResult<R> execute(
					AsyncOperation<AsyncClient, R> op)
					throws ConnectionException {
				// TODO Auto-generated method stub
				return null;
			}
			
		};
	}

}
