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
package com.netflix.astyanax.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.ConnectionFactory;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.IsTimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.ThrottledException;
import com.netflix.astyanax.connectionpool.exceptions.UnknownException;
import com.netflix.astyanax.connectionpool.impl.OperationResultImpl;

public class TestConnectionFactory implements ConnectionFactory<TestClient> {
    private final ConnectionPoolConfiguration config;
    private final ExecutorService executor = Executors.newFixedThreadPool(10,
            new ThreadFactoryBuilder().setDaemon(true).build());
    private final ConnectionPoolMonitor monitor;

    public TestConnectionFactory(ConnectionPoolConfiguration config,
            ConnectionPoolMonitor monitor) {
        this.config = config;
        this.monitor = monitor;
    }

    @Override
    public Connection<TestClient> createConnection(
            final HostConnectionPool<TestClient> pool)
            throws ThrottledException {
        return new Connection<TestClient>() {
            private ConnectionException lastException;
            private boolean isOpen = false;
            private AtomicLong operationCounter = new AtomicLong();

            @Override
            public <R> OperationResult<R> execute(Operation<TestClient, R> op)
                    throws ConnectionException {
                long startTime = System.nanoTime();
                long latency = 0;

                // Execute the operation
                try {
                    TestHostType type = TestHostType.get(getHost().getPort());
                    OperationResult<R> result = type.execute(pool, op);
                    long now = System.nanoTime();
                    latency = now - startTime;
                    pool.addLatencySample(latency, now);
                    return new OperationResultImpl<R>(result.getHost(), result.getResult(), latency);
                } catch (Exception e) {
                    long now = System.nanoTime();
                    latency = now - startTime;
                    ConnectionException connectionException;
                    if (!(e instanceof ConnectionException))
                        connectionException = new UnknownException(e);
                    else 
                        connectionException = (ConnectionException)e;
                    connectionException.setLatency(latency);
                    
                    if (!(connectionException instanceof IsTimeoutException)) {
                        pool.addLatencySample(latency, now);
                    }
                    else {
                        pool.addLatencySample(TimeUnit.NANOSECONDS.convert(config.getSocketTimeout(), TimeUnit.MILLISECONDS), System.nanoTime());
                    }
                    lastException = connectionException;
                    throw lastException;
                }
            }

            @Override
            public void close() {
                if (isOpen) {
                    monitor.incConnectionClosed(getHost(), lastException);
                    executor.submit(new Runnable() {
                        @Override
                        public void run() {
                            final TestHostType type = TestHostType
                                    .get(getHost().getPort());
                            type.close();
                            isOpen = false;
                        }
                    });
                }
            }

            @Override
            public HostConnectionPool<TestClient> getHostConnectionPool() {
                return pool;
            }

            @Override
            public ConnectionException getLastException() {
                return lastException;
            }

            @Override
            public void open() throws ConnectionException {
                TestHostType type = TestHostType.get(getHost().getPort());
                try {
                    type.open(0);
                    isOpen = true;
                    monitor.incConnectionCreated(getHost());
                } catch (ConnectionException e) {
                    lastException = e;
                    e.setHost(getHost());
                    monitor.incConnectionCreateFailed(getHost(), e);
                    throw e;
                }
            }

            @Override
            public void openAsync(final AsyncOpenCallback<TestClient> callback) {
                final Connection<TestClient> This = this;
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        Thread.currentThread().setName("MockConnectionFactory");
                        try {
                            open();
                            callback.success(This);
                        } catch (ConnectionException e) {
                            callback.failure(This, e);
                        } catch (Exception e) {
                            callback.failure(This, new UnknownException(
                                    "Error openning async connection", e));
                        }
                    }
                });
            }

            @Override
            public long getOperationCount() {
                return operationCounter.get();
            }

            @Override
            public Host getHost() {
                return pool.getHost();
            }

            @Override
            public void setMetadata(String key, Object obj) {
                // TODO Auto-generated method stub
                
            }

            @Override
            public Object getMetadata(String key) {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public boolean hasMetadata(String key) {
                // TODO Auto-generated method stub
                return false;
            }
        };
    }
}
