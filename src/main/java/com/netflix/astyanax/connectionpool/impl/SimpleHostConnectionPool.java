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

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.astyanax.connectionpool.BadHostDetector;
import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.ConnectionFactory;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy;
import com.netflix.astyanax.connectionpool.RetryBackoffStrategy;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionAbortedException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.HostDownException;
import com.netflix.astyanax.connectionpool.exceptions.IsDeadConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.IsTimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.MaxConnsPerHostReachedException;
import com.netflix.astyanax.connectionpool.exceptions.PoolTimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.ThrottledException;
import com.netflix.astyanax.connectionpool.exceptions.TimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.UnknownException;

/**
 * Pool of connections for a single host.
 * 
 * Features 1. Async open connection 2.
 * 
 * @author elandau
 * 
 */
public class SimpleHostConnectionPool<CL> implements HostConnectionPool<CL> {

    /**
     * Interface to notify the owning connection pool of up/down state changes.
     * This give the owning connection pool a chance to remove a downed host
     * from its internal state until it's back up.
     * 
     * @author elandau
     * 
     * @param <CL>
     */
    public interface Listener<CL> {
        void onHostDown(HostConnectionPool<CL> pool);

        void onHostUp(HostConnectionPool<CL> pool);
    }

    private static final AtomicLong poolIdCounter = new AtomicLong(0);
    private final long id = poolIdCounter.incrementAndGet();

    private final BlockingQueue<Connection<CL>> availableConnections;
    private final AtomicInteger activeCount = new AtomicInteger(0);
    private final AtomicInteger pendingConnections = new AtomicInteger(0);
    private final AtomicInteger blockedThreads = new AtomicInteger(0);
    private final ConnectionFactory<CL> factory;
    private final Host host;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final ScheduledExecutorService executor = Executors
            .newScheduledThreadPool(1,
                    new ThreadFactoryBuilder().setDaemon(true).build());
    private final RetryBackoffStrategy.Instance retryContext;
    private final BadHostDetector.Instance badHostDetector;
    private final LatencyScoreStrategy.Instance latencyStrategy;
    private final Listener<CL> listener;
    private final ConnectionPoolMonitor monitor;

    protected final ConnectionPoolConfiguration config;

    public SimpleHostConnectionPool(Host host, ConnectionFactory<CL> factory,
            ConnectionPoolMonitor monitor, ConnectionPoolConfiguration config,
            Listener<CL> listener) {
        this.host = host;
        this.config = config;
        this.factory = factory;
        this.listener = listener;
        this.availableConnections = new LinkedBlockingQueue<Connection<CL>>();
        this.retryContext = config.getRetryBackoffStrategy().createInstance();
        this.latencyStrategy = config.getLatencyScoreStrategy()
                .createInstance();
        this.badHostDetector = config.getBadHostDetector().createInstance();
        this.monitor = monitor;
    }

    @Override
    public int growConnections(int numConnections) throws ConnectionException,
            InterruptedException {
        int count = Math.min(numConnections, config.getMaxConnsPerHost());
        for (int i = 0, attemptCount = 0; i < count && attemptCount < 100; i++, attemptCount++) {
            try {
                availableConnections.add(openConnection());
            } catch (MaxConnsPerHostReachedException e) {
                return i;
            } catch (ThrottledException e) {
                Thread.sleep(50);
                i--;
            }
        }
        return count;
    }

    /**
     * Create a connection as long the max hasn't been reached
     * 
     * @param timeout
     *            - Max wait timeout if max connections have been allocated and
     *            pool is empty. 0 to throw a MaxConnsPerHostReachedException.
     * @return
     * @throws TimeoutException
     *             if timeout specified and no new connection is available
     *             MaxConnsPerHostReachedException if max connections created
     *             and no timeout was specified
     */
    @Override
    public Connection<CL> borrowConnection(int timeout)
            throws ConnectionException {
        if (isShutdown()) {
            throw new HostDownException(
                    "Can't borrow connection.  Host is down.");
        }

        Connection<CL> connection = null;
        long startTime = System.currentTimeMillis();
        try {
            // Try to get a free connection without blocking.
            connection = availableConnections.poll();
            if (connection != null) {
                return connection;
            }

            boolean isOpenning = tryOpenAsync();

            // Wait for a connection to free up or a new one to be opened
            if (timeout > 0) {
                connection = waitForConnection(isOpenning ? config
                        .getConnectTimeout() : timeout);
                return connection;
            } else
                throw new PoolTimeoutException(
                        "Fast fail waiting for connection from pool").setHost(
                        getHost()).setLatency(
                        System.currentTimeMillis() - startTime);
        } finally {
            if (connection != null) {
                monitor.incConnectionBorrowed(host, System.currentTimeMillis()
                        - startTime);
            }
        }
    }

    /**
     * Internal method to wait for a connection from the available connection
     * pool.
     * 
     * @param timeout
     * @return
     * @throws ConnectionException
     */
    private Connection<CL> waitForConnection(int timeout)
            throws ConnectionException {
        Connection<CL> connection = null;
        long startTime = System.currentTimeMillis();
        try {
            if (blockedThreads.incrementAndGet() <= config
                    .getMaxBlockedThreadsPerHost()) {
                connection = availableConnections.poll(timeout,
                        TimeUnit.MILLISECONDS);
                if (connection != null)
                    return connection;
            } else {
                throw new PoolTimeoutException(
                        "Too many clients blocked on this pool "
                                + blockedThreads.get()).setHost(getHost());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            blockedThreads.decrementAndGet();
        }

        throw new PoolTimeoutException("Timed out waiting for connection")
                .setHost(getHost()).setLatency(
                        System.currentTimeMillis() - startTime);
    }

    /**
     * Return a connection to this host
     * 
     * @param connection
     */
    @Override
    public boolean returnConnection(Connection<CL> connection) {
        monitor.incConnectionReturned(host);

        ConnectionException ce = connection.getLastException();
        if (ce != null) {
            if (ce instanceof TimeoutException) {
                if (badHostDetector.addTimeoutSample()) {
                    internalCloseConnection(connection);
                    retryContext.suspend();
                    markAsDown(ce);
                    return true;
                }
            }

            if (ce instanceof IsDeadConnectionException) {
                internalCloseConnection(connection);
                if (!(ce instanceof ConnectionAbortedException)) {
                    markAsDown(ce);
                    return true;
                }
                return false;
            }
        }

        // Still within the number of max active connection
        if (activeCount.get() <= config.getMaxConnsPerHost()) {
            availableConnections.add(connection);

            if (isShutdown()) {
                discardIdleConnections();
                return true;
            }
        } else {
            // maxConnsPerHost was reduced. This may end up closing too many
            // connections,
            // but that's ok. We'll open them later.
            internalCloseConnection(connection);
            return true;
        }

        return false;
    }

    @Override
    public boolean closeConnection(Connection<CL> connection) {
        monitor.incConnectionReturned(host);
        internalCloseConnection(connection);
        return true;
    }

    private void internalCloseConnection(Connection<CL> connection) {
        connection.close();
        activeCount.decrementAndGet();
    }

    /**
     * Mark the host as down. No new connections will be created from this host.
     * Connections currently in use will be allowed to continue processing.
     */
    @Override
    public void markAsDown(ConnectionException reason) {
        final HostConnectionPool<CL> pool = this;
        // Start the reconnect thread
        if (isShutdown.compareAndSet(false, true)) {
            discardIdleConnections();
            listener.onHostDown(this);
            monitor.onHostDown(pool.getHost(), reason);

            try {
                executor.schedule(new Runnable() {
                    @Override
                    public void run() {
                        Thread.currentThread().setName(
                                "RetryService : " + host.toString());
                        if (reconnect()) {

                            // Created a new connection successfully. Update
                            // internal state.
                            retryContext.success();
                            isShutdown.set(false);
                            monitor.onHostReactivated(host, pool);
                            listener.onHostUp(SimpleHostConnectionPool.this);
                        } else {
                            executor.schedule(this,
                                    retryContext.getNextDelay(),
                                    TimeUnit.MILLISECONDS);
                        }
                    }
                }, retryContext.getNextDelay(), TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                // Ignore
            }
        } else {
            discardIdleConnections();
        }
    }

    private boolean reconnect() {
        Connection<CL> connection = null;
        try {
            activeCount.incrementAndGet();
            connection = factory
                    .createConnection(SimpleHostConnectionPool.this);
            connection.open();
            availableConnections.add(connection);
            return true;
        } catch (Exception e) {
            activeCount.decrementAndGet();
        }
        return false;
    }

    @Override
    public void shutdown() {
        executor.shutdown();
        markAsDown(null);
    }

    /**
     * Open a new connection synchronously
     * 
     * @return
     * @throws ConnectionException
     */
    @Override
    public Connection<CL> openConnection() throws ConnectionException {
        if (isShutdown()) {
            throw new HostDownException(
                    "Can't open new connection.  Host is down.");
        }

        Connection<CL> connection = null;
        try {
            if (activeCount.incrementAndGet() <= config.getMaxConnsPerHost()) {
                connection = factory.createConnection(this);
                connection.open();
                if (isShutdown()) {
                    connection.close();
                    connection = null;
                    discardIdleConnections();
                    throw new HostDownException(
                            "Host marked down after connection was created.");
                }
                return connection;
            } else {
                throw new PoolTimeoutException("Pool exhausted")
                        .setHost(getHost());
            }
        } catch (Exception e) {
            connection = null;
            ConnectionException ce = (e instanceof ConnectionException) ? (ConnectionException) e
                    : new UnknownException(e);
            if (ce instanceof IsDeadConnectionException) {
                markAsDown(ce);
            }
            throw ce;
        } finally {
            if (connection == null) {
                activeCount.decrementAndGet();
            }
        }
    }

    /**
     * Try to open a new connection asynchronously. We don't actually return a
     * connection here. Instead, the connection will be added to idle queue when
     * it's ready.
     */
    private boolean tryOpenAsync() {
        Connection<CL> connection = null;
        // Try to open a new connection, as long as we haven't reached the max
        try {
            if (activeCount.incrementAndGet() <= config.getMaxConnsPerHost()) {
                // Don't try to open too many connections at the same time.
                if (pendingConnections.incrementAndGet() > config
                        .getMaxPendingConnectionsPerHost()) {
                    pendingConnections.decrementAndGet();
                } else {
                    try {
                        connection = factory.createConnection(this);
                        connection
                                .openAsync(new Connection.AsyncOpenCallback<CL>() {
                                    @Override
                                    public void success(
                                            Connection<CL> connection) {
                                        pendingConnections.decrementAndGet();
                                        availableConnections.add(connection);

                                        // Sanity check in case the connection
                                        // pool was closed
                                        if (isShutdown()) {
                                            discardIdleConnections();
                                        }
                                    }

                                    @Override
                                    public void failure(Connection<CL> conn,
                                            ConnectionException e) {
                                        pendingConnections.decrementAndGet();
                                        activeCount.decrementAndGet();

                                        if (e instanceof IsDeadConnectionException) {
                                            markAsDown(e);
                                        }
                                    }
                                });
                        return true;
                    } catch (ThrottledException e) {
                        // Trying to open way too many connections here
                    } finally {
                        if (connection == null) {
                            pendingConnections.decrementAndGet();
                        }
                    }
                }
            }
        } finally {
            if (connection == null) {
                activeCount.decrementAndGet();
            }
        }
        return false;
    }

    @Override
    public boolean isShutdown() {
        return isShutdown.get();
    }

    @Override
    public Host getHost() {
        return host;
    }

    @Override
    public int getActiveConnectionCount() {
        return activeCount.get();
    }

    @Override
    public int getIdleConnectionCount() {
        return availableConnections.size();
    }

    @Override
    public int getPendingConnectionCount() {
        return pendingConnections.get();
    }

    @Override
    public int getBlockedThreadCount() {
        return blockedThreads.get();
    }

    @Override
    public int getBusyConnectionCount() {
        return getActiveConnectionCount() - getIdleConnectionCount()
                - getPendingConnectionCount();
    }

    @Override
    public double getScore() {
        return latencyStrategy.getScore();
    }

    @Override
    public double getMeanLatency() {
        return latencyStrategy.getMean();
    }

    @Override
    public void addLatencySample(long latency, long now) {
        latencyStrategy.addSample(latency);
    }

    private void discardIdleConnections() {
        List<Connection<CL>> connections = Lists.newArrayList();
        availableConnections.drainTo(connections);
        activeCount.addAndGet(-connections.size());

        for (Connection<CL> connection : connections) {
            connection.close(); // This is usually an async operation
        }
    }

    public String toString() {
        int idle = getIdleConnectionCount();
        int open = getActiveConnectionCount();
        return new StringBuilder().append("SimpleHostConnectionPool[")
                .append("host=").append(host).append("-").append(id)
                .append(",active=").append(!isShutdown()).append(",open=")
                .append(open).append(",busy=").append(open - idle)
                .append(",idle=").append(idle).append(",blocked=")
                .append(getBlockedThreadCount()).append(",pending=")
                .append(getPendingConnectionCount()).append(",score=")
                .append(getScore()).append("]").toString();
    }
}
