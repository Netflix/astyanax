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
package com.netflix.astyanax.thrift;

import com.netflix.astyanax.connectionpool.*;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.TimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.TransportException;
import com.netflix.astyanax.connectionpool.impl.OperationResultImpl;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Factory to create Cassandra.Client connections.  All connections 
 * 
 * @author elandau
 * <C> Client class type
 * <D> ConnectionData (context)
 */
public abstract class ThriftConnectionFactoryImpl<CL, D> implements ConnectionFactory<CL> {
	
	private final AtomicLong idCounter = new AtomicLong(0);
    private final String nameFormat;

    protected final ConnectionPoolConfiguration config;

    protected ThriftConnectionFactoryImpl(ConnectionPoolConfiguration config, String nameFormat) {
		this.config = config;
        this.nameFormat = nameFormat;
    }

    abstract protected void setKeyspace(CL client, String keyspaceName) throws TException, InvalidRequestException;

    abstract protected CL createClient(HostConnectionPool<CL> pool, AtomicReference<D> connectionDataHolder) throws ConnectionException;

    abstract protected void closeClient(CL client, D connectionData);

    abstract protected boolean clientIsOpen(CL client, D connectionData);

	@Override
	public Connection<CL> createConnection(
			final HostConnectionPool<CL> pool) {
		return new Connection<CL>() {
			private final long id = idCounter.incrementAndGet();
			private CL cassandraClient;
			private ConnectionException lastException = null;
			private String keyspaceName;
            private D connectionData;
			
			@Override
			public <R> OperationResult<R> execute(
					Operation<CL, R> op) throws ConnectionException {
				try {
					if (op.getKeyspace() != null && op.getKeyspace().compareTo(keyspaceName) != 0) {
						this.keyspaceName = op.getKeyspace();
						try {
                            setKeyspace(cassandraClient, keyspaceName);
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
			public void open() throws ConnectionException {
				// 1.  Is it already open?
				if (this.cassandraClient != null && isOpen()) {
					throw new IllegalStateException("Open called on already open connection");
				}

                AtomicReference<D> connectionDataHolder = new AtomicReference<D>();
				this.cassandraClient = createClient(pool, connectionDataHolder);
                connectionData = connectionDataHolder.get();
				try {
					if (config.getKeyspaceName() != null) {
						this.keyspaceName = config.getKeyspaceName();
                        setKeyspace(cassandraClient, keyspaceName);
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
                    closeClient(cassandraClient, connectionData);
				}
			}

			@Override
			public boolean isOpen() {
				return clientIsOpen(cassandraClient, connectionData);
			}

			@Override
			public HostConnectionPool<CL> getHostConnectionPool() {
				return pool;
			}

			@Override
			public ConnectionException getLastException() {
				return this.lastException;
			}
			
			@Override
			public String toString() {
			    return String.format(nameFormat, pool.getHost().getHostName(), id);
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
