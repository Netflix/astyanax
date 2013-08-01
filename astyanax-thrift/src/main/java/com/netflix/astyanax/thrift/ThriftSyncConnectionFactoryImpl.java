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

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AuthenticationCredentials;
import com.netflix.astyanax.CassandraOperationTracer;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.KeyspaceTracerFactory;
import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.ConnectionFactory;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.RateLimiter;
import com.netflix.astyanax.connectionpool.SSLConnectionContext;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.IsTimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.ThrottledException;
import com.netflix.astyanax.connectionpool.impl.OperationResultImpl;
import com.netflix.astyanax.connectionpool.impl.SimpleRateLimiterImpl;

import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ThriftSyncConnectionFactoryImpl implements ConnectionFactory<Cassandra.Client> {
    private static final String NAME_FORMAT = "ThriftConnection<%s-%d>";
    private static final Logger LOG = LoggerFactory.getLogger(ThriftSyncConnectionFactoryImpl.class);
    private final static ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true)
            .build());
    
    private final AtomicLong idCounter = new AtomicLong(0);
    private final RateLimiter limiter;
    private final ConnectionPoolConfiguration cpConfig;
    private final KeyspaceTracerFactory tracerFactory;
    private final ConnectionPoolMonitor monitor;
    private final AstyanaxConfiguration asConfig;

    public ThriftSyncConnectionFactoryImpl(AstyanaxConfiguration asConfig, ConnectionPoolConfiguration cpConfig, KeyspaceTracerFactory tracerFactory,
            ConnectionPoolMonitor monitor) {
        this.cpConfig = cpConfig;
        this.asConfig = asConfig;
        this.limiter = new SimpleRateLimiterImpl(cpConfig);
        this.tracerFactory = tracerFactory;
        this.monitor = monitor;
    }

    @Override
    public Connection<Cassandra.Client> createConnection(final HostConnectionPool<Cassandra.Client> pool)
            throws ThrottledException {
        if (limiter.check() == false) {
            throw new ThrottledException("Too many connection attempts");
        }

        return new ThriftConnection(pool);
    }
    
    public class ThriftConnection implements Connection<Cassandra.Client> {
        private final long id = idCounter.incrementAndGet();
        private Cassandra.Client cassandraClient;
        private TFramedTransport transport;
        private TSocket socket;
        private int timeout = 0;
        private AtomicLong operationCounter = new AtomicLong();
        private AtomicBoolean closed = new AtomicBoolean(false);

        private volatile ConnectionException lastException = null;
        private volatile String keyspaceName;

        private final HostConnectionPool<Cassandra.Client> pool;

        private Map<String, Object> metadata = Maps.newHashMap();
        
        public ThriftConnection(HostConnectionPool<Cassandra.Client> pool) {
            this.pool = pool;
        }
        
        @Override
        public <R> OperationResult<R> execute(Operation<Cassandra.Client, R> op) throws ConnectionException {
            long startTime = System.nanoTime();
            long latency = 0;
            setTimeout(cpConfig.getSocketTimeout()); // In case the configurationchanged
            operationCounter.incrementAndGet();

            // Set a new keyspace, if it changed
            lastException = null;
            if (op.getKeyspace() != null && (keyspaceName == null || !op.getKeyspace().equals(keyspaceName))) {
                CassandraOperationTracer tracer = tracerFactory.newTracer(CassandraOperationType.SET_KEYSPACE)
                        .start();
                try {
                    cassandraClient.set_keyspace(op.getKeyspace());
                    if (asConfig.getCqlVersion() != null)
                        cassandraClient.set_cql_version(asConfig.getCqlVersion());
                    keyspaceName = op.getKeyspace();
                    long now = System.nanoTime();
                    latency = now - startTime;
                    pool.addLatencySample(latency, now);
                    tracer.success();
                }
                catch (Exception e) {
                    long now = System.nanoTime();
                    latency = now - startTime;
                    lastException = ThriftConverter.ToConnectionPoolException(e).setLatency(latency);
                    if (e instanceof IsTimeoutException) {
                        pool.addLatencySample(TimeUnit.NANOSECONDS.convert(cpConfig.getSocketTimeout(), TimeUnit.MILLISECONDS), now);
                    }
                    tracer.failure(lastException);
                    throw lastException;
                }
                startTime = System.nanoTime(); // We don't want to include
                                               // the set_keyspace in our
                                               // latency calculation
            }

            // Execute the operation
            try {
                R result = op.execute(cassandraClient, this);
                long now = System.nanoTime();
                latency = now - startTime;
                pool.addLatencySample(latency, now);
                return new OperationResultImpl<R>(getHost(), result, latency);
            }
            catch (Exception e) {
                long now = System.nanoTime();
                latency = now - startTime;
                lastException = ThriftConverter.ToConnectionPoolException(e).setLatency(latency);
                if (e instanceof IsTimeoutException) {
                    pool.addLatencySample(TimeUnit.NANOSECONDS.convert(cpConfig.getSocketTimeout(), TimeUnit.MILLISECONDS), now);
                }
                throw lastException;
            }
        }

        @Override
        public void open() throws ConnectionException {
            if (cassandraClient != null) {
                throw new IllegalStateException("Open called on already open connection");
            }

            long startTime = System.currentTimeMillis();
            try {
                final SSLConnectionContext sslCxt = cpConfig.getSSLConnectionContext();
                if(sslCxt != null) {
                    TSSLTransportParameters params = new TSSLTransportParameters(sslCxt.getSslProtocol(), sslCxt.getSslCipherSuites().toArray(new String[0]));
                    params.setTrustStore(sslCxt.getSslTruststore(), sslCxt.getSslTruststorePassword());
                    //thrift's SSL implementation does not allow you set the socket connect timeout, only read timeout
                    socket = TSSLTransportFactory.getClientSocket(getHost().getIpAddress(), getHost().getPort(), cpConfig.getSocketTimeout(), params);
                } else {
                    socket = new TSocket(getHost().getIpAddress(), getHost().getPort(), cpConfig.getConnectTimeout());
                }

                socket.getSocket().setTcpNoDelay(true);
                socket.getSocket().setKeepAlive(true);
                socket.getSocket().setSoLinger(false, 0);

                setTimeout(cpConfig.getSocketTimeout());
                transport = new TFramedTransport(socket);
                if(!transport.isOpen())
                    transport.open();

                cassandraClient = new Cassandra.Client(new TBinaryProtocol.Factory().getProtocol(transport));
                monitor.incConnectionCreated(getHost());

                AuthenticationCredentials credentials = cpConfig.getAuthenticationCredentials();
                if (credentials != null) {
                    Map<String, String> thriftCredentials = Maps.newHashMapWithExpectedSize(2);
                    thriftCredentials.put("username", credentials.getUsername());
                    thriftCredentials.put("password", credentials.getPassword());
                    cassandraClient.login(new AuthenticationRequest(thriftCredentials));
                }
            }
            catch (Exception e) {
                pool.addLatencySample(TimeUnit.NANOSECONDS.convert(cpConfig.getSocketTimeout(), TimeUnit.MILLISECONDS), System.nanoTime());
                closeClient();
                ConnectionException ce = ThriftConverter.ToConnectionPoolException(e).setHost(getHost())
                        .setLatency(System.currentTimeMillis() - startTime);
                monitor.incConnectionCreateFailed(getHost(), ce);
                throw ce;
            }
            catch (Throwable t) {
                LOG.error("Error creating connection", t);
                pool.addLatencySample(TimeUnit.NANOSECONDS.convert(cpConfig.getSocketTimeout(), TimeUnit.MILLISECONDS), System.nanoTime());
                closeClient();
                ConnectionException ce = ThriftConverter.ToConnectionPoolException(new RuntimeException("Error openning connection", t)).setHost(getHost())
                        .setLatency(System.currentTimeMillis() - startTime);
                monitor.incConnectionCreateFailed(getHost(), ce);
                throw ce;
            }
        }

        @Override
        public void openAsync(final AsyncOpenCallback<Cassandra.Client> callback) {
            final Connection<Cassandra.Client> This = this;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        open();
                        callback.success(This);
                    }
                    catch (Exception e) {
                        pool.addLatencySample(TimeUnit.NANOSECONDS.convert(cpConfig.getSocketTimeout(), TimeUnit.MILLISECONDS), System.nanoTime());
                        callback.failure(This, ThriftConverter.ToConnectionPoolException(e));
                    }
                }
            });

        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                monitor.incConnectionClosed(getHost(), lastException);
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            closeClient();
                        }
                        catch (Exception e) {
                        }
                    }
                });
            }
        }

        private void closeClient() {
            if (transport != null) {
                try {
                    transport.flush();
                }
                catch (TTransportException e) {
                }
                finally {
                    try {
                        transport.close();
                    }
                    catch (Exception e) {
                    }
                    finally {
                        transport = null;
                    }
                }
            }

            if (socket != null) {
                try {
                    socket.close();
                }
                catch (Exception e) {
                }
                finally {
                    socket = null;
                }
            }
        }

        @Override
        public HostConnectionPool<Cassandra.Client> getHostConnectionPool() {
            return pool;
        }

        @Override
        public ConnectionException getLastException() {
            return lastException;
        }

        @Override
        public String toString() {
            return String.format(NAME_FORMAT, getHost().getHostName(), id);
        }

        /**
         * Compares the toString of these clients
         */
        @Override
        public boolean equals(Object obj) {
            return toString().equals(obj.toString());
        }

        @Override
        public long getOperationCount() {
            return operationCounter.get();
        }

        @Override
        public Host getHost() {
            return pool.getHost();
        }

        public void setTimeout(int timeout) {
            if (this.timeout != timeout) {
                socket.setTimeout(timeout);
                this.timeout = timeout;
            }
        }

        @Override
        public void setMetadata(String key, Object data) {
            metadata.put(key, data);
        }

        @Override
        public Object getMetadata(String key) {
            return metadata.get(key);
        }

        @Override
        public boolean hasMetadata(String key) {
            return metadata.containsKey(key);
        }
    }
}
