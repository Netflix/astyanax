package com.netflix.astyanax.thrift;

import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.netflix.astyanax.connectionpool.AsyncOperation;
import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.ConnectionFactory;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.FutureOperationResult;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.TimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.TransportException;
import com.netflix.astyanax.connectionpool.impl.OperationResultImpl;

/**
 * Factory to create Cassandra.Client connections.  All connections 
 * 
 * @author elandau
 *
 */
public class ThriftConnectionFactoryImpl implements ConnectionFactory<Cassandra.Client> {
	
	private static final String NAME_FORMAT = "ThriftConnection<%s-%d>";
	private final AtomicLong idCounter = new AtomicLong(0);
	private final ConnectionPoolConfiguration config;
	
	public ThriftConnectionFactoryImpl(ConnectionPoolConfiguration config) {
		this.config = config;
	}
	
	@Override
	public Connection<Client> createConnection(
			final HostConnectionPool<Client> pool) {
		return new Connection<Cassandra.Client>() {
			private final long id = idCounter.incrementAndGet();
			private TTransport transport;
			private Cassandra.Client cassandraClient;
			private ConnectionException lastException = null;
			private String keyspaceName;
			
			@Override
			public <R> OperationResult<R> execute(
					Operation<Cassandra.Client, R> op) throws ConnectionException {
				try {
					if (op.getKeyspace() != null && op.getKeyspace().compareTo(keyspaceName) != 0) {
						this.keyspaceName = op.getKeyspace();
						try {
							cassandraClient.set_keyspace(keyspaceName);
						} catch (Exception e) {
							throw ThriftConverter.ToConnectionPoolException(e);
						}
					}
					lastException = null;
					long latency = System.nanoTime();
					R result = op.execute(this.cassandraClient);
					latency = System.nanoTime() - latency;
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
			public <R> FutureOperationResult<R> execute(
					AsyncOperation<Client, R> op)
					throws ConnectionException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void open() throws ConnectionException, BadRequestException {
				// 1.  Is it already open?
				if (isOpen()) {
					throw new IllegalStateException("Open called on already open connection");
				}
				
				// 2.  Attempt to connect the socket
				TSocket socket = null;
		        try {
					socket = new TSocket(pool.getHost().getIpAddress(), 
							pool.getHost().getPort(), config.getSocketTimeout());

			        this.transport = new TFramedTransport(socket);
			        this.transport.open();
		        } 
			    catch (TTransportException e) {
			    	// Thrift exceptions aren't very good in reporting, so we have to catch the exception here and
			    	// add details to it.
			    	throw new TransportException("Failed to open transport", e);	// TODO
			    }
			    
			    // 3.  Create a client object instance and set the keyspace
				this.cassandraClient = new Cassandra.Client(new TBinaryProtocol(transport));
				try {
					if (config.getKeyspaceName() != null) {
						this.keyspaceName = config.getKeyspaceName();
						this.cassandraClient.set_keyspace(this.keyspaceName);
					}
		        	// TODO: What about timeout exceptions
				} 
				catch (TTransportException e) {
					if (e.getCause() instanceof SocketTimeoutException) {
						throw new TimeoutException(pool.getHost() + " Timed out trying to set keyspace " + this.keyspaceName, e);
					}
			    	throw new com.netflix.astyanax.connectionpool.exceptions.BadRequestException(pool.getHost().getName() + " Failed to set keyspace: " + keyspaceName, e);	// TODO
				}
				catch (InvalidRequestException e) {
			    	throw new com.netflix.astyanax.connectionpool.exceptions.BadRequestException(pool.getHost().getName() + " Failed to set keyspace: " + keyspaceName, e);	// TODO
				} 
				catch (TException e) {
			    	throw new com.netflix.astyanax.connectionpool.exceptions.BadRequestException(pool.getHost().getName() + " Failed to set keyspace: " + keyspaceName, e);	// TODO
				}			
			}
			
			@Override
			public void close() {
				if (isOpen()) {
					try {
						transport.flush();
					} catch (TTransportException e) {
					} 
					finally {
						try {
							transport.close();
						}
						finally {
						}
					}
				}
			}

			@Override
			public boolean isOpen() {
				return transport != null && transport.isOpen();
			}

			@Override
			public HostConnectionPool<Client> getHostConnectionPool() {
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

			/**
			 * Compares the toString of these clients   
			 */
			@Override
			public boolean equals(Object obj) {
				return this.toString().equals(obj.toString());
			}
		};
	}
}
