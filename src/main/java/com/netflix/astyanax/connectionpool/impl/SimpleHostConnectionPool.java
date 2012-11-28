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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.InterruptedOperationException;
import com.netflix.astyanax.connectionpool.exceptions.HostDownException;
import com.netflix.astyanax.connectionpool.exceptions.IsDeadConnectionException;
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
    private final static Logger LOG = LoggerFactory.getLogger(SimpleHostConnectionPool.class);
    private final static int MAX_PRIME_CONNECTIONS_RETRY_ATTEMPT = 2;
    private final static int PRIME_CONNECTION_DELAY = 100;

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

    private static final AtomicLong             poolIdCounter = new AtomicLong(0);
    private final long                          id = poolIdCounter.incrementAndGet();

    private final BlockingQueue<Connection<CL>> availableConnections;
    private final AtomicInteger                 activeCount          = new AtomicInteger(0);
    private final AtomicInteger                 pendingConnections   = new AtomicInteger(0);
    private final AtomicInteger                 blockedThreads       = new AtomicInteger(0);
    private final AtomicInteger                 openConnections      = new AtomicInteger(0);
    private final AtomicInteger                 failedOpenConnections= new AtomicInteger(0);
    private final AtomicInteger                 closedConnections    = new AtomicInteger(0);
    private final AtomicLong                    borrowedCount        = new AtomicLong(0);
    private final AtomicLong                    returnedCount        = new AtomicLong(0);
    private final AtomicInteger                 connectAttempt       = new AtomicInteger(0);
    
    private final AtomicInteger                 errorsSinceLastSuccess = new AtomicInteger(0);

    private final ConnectionFactory<CL>         factory;
    private final Host                          host;
    private final AtomicBoolean                 isShutdown           = new AtomicBoolean(false);
    private final AtomicBoolean                 isReconnecting       = new AtomicBoolean(false);
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).build());
    private final RetryBackoffStrategy.Instance retryContext;
    private final BadHostDetector.Instance      badHostDetector;
    private final LatencyScoreStrategy.Instance latencyStrategy;
    private final Listener<CL>                  listener;
    private final ConnectionPoolMonitor         monitor;

    protected final ConnectionPoolConfiguration config;

    public SimpleHostConnectionPool(Host host, ConnectionFactory<CL> factory, ConnectionPoolMonitor monitor,
            ConnectionPoolConfiguration config, Listener<CL> listener) {
        
        this.host            = host;
        this.config          = config;
        this.factory         = factory;
        this.listener        = listener;
        this.retryContext    = config.getRetryBackoffStrategy().createInstance();
        this.latencyStrategy = config.getLatencyScoreStrategy().createInstance();
        this.badHostDetector = config.getBadHostDetector().createInstance();
        this.monitor         = monitor;
        this.availableConnections = new LinkedBlockingQueue<Connection<CL>>();
    }

    @Override
    public int primeConnections(int numConnections) throws ConnectionException, InterruptedException {
        if (isReconnecting()) {
            throw new HostDownException("Can't prime connections on downed host.");
        }
        // Don't try to create more than we're allowed
        int remaining = Math.min(numConnections, config.getMaxConnsPerHost() - getActiveConnectionCount());
        
        // Attempt to open 'count' connections and allow for MAX_PRIME_CONNECTIONS_RETRY_ATTEMPT
        // retries before giving up if we can't open more.
        int opened = 0;
        Exception lastException = null;
        for (int i = 0; opened < remaining && i < MAX_PRIME_CONNECTIONS_RETRY_ATTEMPT;) {
            try {
                reconnect();
                opened++;
            }
            catch (Exception e) {
                lastException = e;
                Thread.sleep(PRIME_CONNECTION_DELAY);
                i++;
            }
        }
        
        // If no connection was opened then mark this host as down
        if (remaining > 0 && opened == 0) {
            this.markAsDown(null);
            throw new HostDownException("Failed to prime connections", lastException);
        }
        return opened;
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
    public Connection<CL> borrowConnection(int timeout) throws ConnectionException {
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
                connection = waitForConnection(isOpenning ? config.getConnectTimeout() : timeout);
                return connection;
            }
            else
                throw new PoolTimeoutException("Fast fail waiting for connection from pool")
                        .setHost(getHost())
                        .setLatency(System.currentTimeMillis() - startTime);
        }
        finally {
            if (connection != null) {
                borrowedCount.incrementAndGet();
                monitor.incConnectionBorrowed(host, System.currentTimeMillis() - startTime);
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
    private Connection<CL> waitForConnection(int timeout) throws ConnectionException {
        Connection<CL> connection = null;
        long startTime = System.currentTimeMillis();
        try {
            blockedThreads.incrementAndGet();
            connection = availableConnections.poll(timeout, TimeUnit.MILLISECONDS);
            if (connection != null)
                return connection;
            
            throw new PoolTimeoutException("Timed out waiting for connection")
                .setHost(getHost())
                .setLatency(System.currentTimeMillis() - startTime);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedOperationException("Thread interrupted waiting for connection")
                .setHost(getHost())
                .setLatency(System.currentTimeMillis() - startTime);
        }
        finally {
            blockedThreads.decrementAndGet();
        }
    }

    /**
     * Return a connection to this host
     * 
     * @param connection
     */
    @Override
    public boolean returnConnection(Connection<CL> connection) {
        returnedCount.incrementAndGet();
        monitor.incConnectionReturned(host);

        ConnectionException ce = connection.getLastException();
        if (ce != null) {
            if (ce instanceof IsDeadConnectionException) {
                noteError(ce);
                internalCloseConnection(connection);
                return true;
            }
        }
        errorsSinceLastSuccess.set(0);

        // Still within the number of max active connection
        if (activeCount.get() <= config.getMaxConnsPerHost()) {
            availableConnections.add(connection);

            if (isShutdown()) {
                discardIdleConnections();
                return true;
            }
        }
        else {
            // maxConnsPerHost was reduced. This may end up closing too many
            // connections, but that's ok. We'll open them later.
            internalCloseConnection(connection);
            return true;
        }

        return false;
    }

    @Override
    public boolean closeConnection(Connection<CL> connection) {
        returnedCount.incrementAndGet();
        monitor.incConnectionReturned(host);
        internalCloseConnection(connection);
        return true;
    }

    private void internalCloseConnection(Connection<CL> connection) {
        try {
            closedConnections.incrementAndGet();
            connection.close();
        }
        finally {
            activeCount.decrementAndGet();
        }
    }

    private void noteError(ConnectionException reason) {
        if (errorsSinceLastSuccess.incrementAndGet() > 3) 
            markAsDown(reason);
    }
    
    /**
     * Mark the host as down. No new connections will be created from this host.
     * Connections currently in use will be allowed to continue processing.
     */
    @Override
    public void markAsDown(ConnectionException reason) {
        // Make sure we're not triggering the reconnect process more than once
        if (isReconnecting.compareAndSet(false, true)) {
            if (reason != null && !(reason instanceof TimeoutException)) {
                discardIdleConnections();
            }
            
            listener.onHostDown(this);
            monitor .onHostDown(getHost(), reason);

            retryContext.begin();
            
            try {
                executor.schedule(new Runnable() {
                    @Override
                    public void run() {
                        Thread.currentThread().setName("RetryService : " + host.getName());
                        try {
                            if (activeCount.get() == 0)
                                reconnect();
                            
                            // Created a new connection successfully.
                            try {
                                retryContext.success();
                                if (isReconnecting.compareAndSet(true, false)) {
                                    monitor .onHostReactivated(host, SimpleHostConnectionPool.this);
                                    listener.onHostUp(SimpleHostConnectionPool.this);
                                }
                            }
                            catch (Throwable t) {
//                                t.printStackTrace();
                            }
                            return;
                        }
                        catch (Throwable t) {
                            // Ignore
//                            t.printStackTrace();
                        }
                        
                        if (!isShutdown()) {
                            long delay = retryContext.getNextDelay();
                            executor.schedule(this, delay, TimeUnit.MILLISECONDS);
                        }
                    }
                }, retryContext.getNextDelay(), TimeUnit.MILLISECONDS);
            }
            catch (RejectedExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void reconnect() throws Exception {
        try {
            if (activeCount.get() < config.getMaxConnsPerHost()) {
                if (activeCount.incrementAndGet() <= config.getMaxConnsPerHost()) {
                    connectAttempt.incrementAndGet();
                    Connection<CL> connection = factory.createConnection(SimpleHostConnectionPool.this);
                    connection.open();
                    
                    errorsSinceLastSuccess.set(0);
                    availableConnections.add(connection);
                    openConnections.incrementAndGet();
                }
                else {
                    activeCount.decrementAndGet();
                }
            }
        }
        catch (ConnectionException e) {
            failedOpenConnections.incrementAndGet();
            activeCount.decrementAndGet();
            noteError(e);
            throw e;
        }
        catch (Throwable t) {
            failedOpenConnections.incrementAndGet();
            activeCount.decrementAndGet();
            ConnectionException ce = new UnknownException(t);
            noteError(ce);
            throw ce;
        }
    }

    @Override
    public void shutdown() {
        executor.shutdownNow();
        isReconnecting.set(true);
        isShutdown.set(true);
        discardIdleConnections();
        
        config.getLatencyScoreStrategy().removeInstance(this.latencyStrategy);
        config.getBadHostDetector().removeInstance(this.badHostDetector);
    }

    /**
     * Try to open a new connection asynchronously. We don't actually return a
     * connection here. Instead, the connection will be added to idle queue when
     * it's ready.
     */
    private boolean tryOpenAsync() {
        Connection<CL> connection = null;
        // Try to open a new connection, as long as we haven't reached the max
        if (activeCount.get() < config.getMaxConnsPerHost()) {
            try {
                if (activeCount.incrementAndGet() <= config.getMaxConnsPerHost()) {
                    // Don't try to open too many connections at the same time.
                    if (pendingConnections.incrementAndGet() > config.getMaxPendingConnectionsPerHost()) {
                        pendingConnections.decrementAndGet();
                    }
                    else {
                        try {
                            connectAttempt.incrementAndGet();
                            connection = factory.createConnection(this);
                            connection.openAsync(new Connection.AsyncOpenCallback<CL>() {
                                @Override
                                public void success(Connection<CL> connection) {
                                    openConnections.incrementAndGet();
                                    pendingConnections.decrementAndGet();
                                    availableConnections.add(connection);
    
                                    // Sanity check in case the connection
                                    // pool was closed
                                    if (isShutdown()) {
                                        discardIdleConnections();
                                    }
                                }
    
                                @Override
                                public void failure(Connection<CL> conn, ConnectionException e) {
                                    failedOpenConnections.incrementAndGet();
                                    pendingConnections.decrementAndGet();
                                    activeCount.decrementAndGet();
    
                                    if (e instanceof IsDeadConnectionException) {
                                        noteError(e);
                                    }
                                }
                            });
                            return true;
                        }
                        catch (ThrottledException e) {
                            // Trying to open way too many connections here
                        }
                        finally {
                            if (connection == null)
                                pendingConnections.decrementAndGet();
                        }
                    }
                }
            }
            finally {
                if (connection == null) {
                    activeCount.decrementAndGet();
                }
            }
        }
        return false;
    }

    @Override
    public boolean isShutdown() {
        return isShutdown.get();
    }

    public boolean isReconnecting() {
        return isReconnecting.get();
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
    public int getOpenedConnectionCount() {
        return openConnections.get();
    }
    
    @Override
    public int getFailedOpenConnectionCount() {
        return failedOpenConnections.get();
    }
    
    @Override
    public int getClosedConnectionCount() {
        return closedConnections.get();
    }
    
    @Override
    public int getConnectAttemptCount() {
        return this.connectAttempt.get();
    }

    
    @Override
    public int getBusyConnectionCount() {
        return getActiveConnectionCount() - getIdleConnectionCount() - getPendingConnectionCount();
    }

    @Override
    public double getScore() {
        return latencyStrategy.getScore();
    }

    @Override
    public void addLatencySample(long latency, long now) {
        latencyStrategy.addSample(latency);
    }
    
    @Override
    public int getErrorsSinceLastSuccess() {
        return errorsSinceLastSuccess.get();
    }

    /**
     * Drain all idle connections and close them.  Connections that are currently borrowed
     * will not be closed here.
     */
    private void discardIdleConnections() {
        List<Connection<CL>> connections = Lists.newArrayList();
        availableConnections.drainTo(connections);
        activeCount.addAndGet(-connections.size());

        for (Connection<CL> connection : connections) {
            try {
                closedConnections.incrementAndGet();
                connection.close(); // This is usually an async operation
            }
            catch (Throwable t) {
                // TODO
            }
        }
    }

    public String toString() {
        int idle = getIdleConnectionCount();
        int open = getActiveConnectionCount();
        return new StringBuilder()
                .append("SimpleHostConnectionPool[")
                .append("host="    ).append(host).append("-").append(id)
                .append(",active=" ).append(!isReconnecting())
                .append(",connections(")
                .append(  "open=").append(open)
                .append( ",idle="   ).append(idle)
                .append( ",busy="   ).append(open - idle)
                .append( ",closed=").append(closedConnections.get())
                .append( ",failed=").append(failedOpenConnections.get())
                .append(")")
                .append(",borrow=" ).append(borrowedCount.get())
                .append(",return=" ).append(returnedCount.get())
                .append(",blocked=").append(getBlockedThreadCount())
                .append(",pending=").append(getPendingConnectionCount())
                .append(",score="  ).append(getScore()/1000000)
                .append("]").toString();
    }

    @Override
    public boolean isActive() {
        return !this.isShutdown.get();
    }
}
