package com.netflix.astyanax.connectionpool.impl;

import java.util.List;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.ConnectionFactory;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.ExecuteWithFailover;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.IsDeadConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.IsTimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.NoAvailableHostsException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.exceptions.TimeoutException;

/**
 * Connection pool which puts all connections in a single queue. The load
 * balancing is essentially random here.
 * 
 * @author elandau
 * 
 * @param <CL>
 */
public class BagOfConnectionsConnectionPoolImpl<CL> extends
        AbstractHostPartitionConnectionPool<CL> {

    private final LinkedBlockingQueue<Connection<CL>> idleConnections = new LinkedBlockingQueue<Connection<CL>>();
    private final AtomicInteger activeConnectionCount = new AtomicInteger(0);
    private final Random randomIndex = new Random();

    public BagOfConnectionsConnectionPoolImpl(
            ConnectionPoolConfiguration config, ConnectionFactory<CL> factory,
            ConnectionPoolMonitor monitor) {
        super(config, factory, monitor);
    }

    private <R> Connection<CL> borrowConnection(Operation<CL, R> op)
            throws ConnectionException, OperationException {
        long startTime = System.currentTimeMillis();

        // Try to get an open connection from the bag
        Connection<CL> connection = null;

        boolean newConnection = false;
        try {
            connection = idleConnections.poll();
            if (connection != null) {
                return connection;
            }

            // Already reached max connections so just wait
            if (activeConnectionCount.incrementAndGet() > config.getMaxConns()) {
                activeConnectionCount.decrementAndGet();
                try {
                    connection = idleConnections.poll(
                            config.getMaxTimeoutWhenExhausted(),
                            TimeUnit.MILLISECONDS);
                    if (connection == null) {
                        this.monitor.incPoolExhaustedTimeout();
                        throw new TimeoutException(
                                "Timed out waiting for connection from bag");
                    }
                    return connection;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new TimeoutException(
                            "Interrupted waiting to borrow a connection");
                }
            }
            // Try to create a new one
            try {
                newConnection = true;
                // Find a random node and open a connection on it. If that node
                // has been exhausted
                // then try the next one in array order until a connection can
                // be created
                List<HostConnectionPool<CL>> pools = topology.getAllPools()
                        .getPools();
                if (pools != null && pools.size() > 0) {
                    int index = randomIndex.nextInt(pools.size());
                    for (int i = 0; i < pools.size(); ++i, ++index) {
                        HostConnectionPool<CL> pool = pools.get(index
                                % pools.size());
                        try {
                            connection = pool.borrowConnection(config
                                    .getConnectTimeout());
                            return connection;
                        } catch (ConnectionException connectionException) {
                            // Ignore
                        }
                    }
                    monitor.incNoHosts();
                    throw new NoAvailableHostsException(
                            "Too many errors trying to open a connection");
                } else {
                    monitor.incNoHosts();
                    throw new NoAvailableHostsException(
                            "No hosts to borrow from");
                }
            } finally {
                if (connection == null)
                    activeConnectionCount.decrementAndGet();
            }
        } finally {
            if (connection != null && newConnection == false)
                monitor.incConnectionBorrowed(connection
                        .getHostConnectionPool().getHost(),
                        System.currentTimeMillis() - startTime);
        }
    }

    protected boolean returnConnection(Connection<CL> connection) {
        if (connection != null) {
            if (connection.getHostConnectionPool().isShutdown()
                    || connection.getOperationCount() > config
                            .getMaxOperationsPerConnection()) {
                closeConnection(connection);
            } else {
                ConnectionException ce = connection.getLastException();
                if (ce != null
                        && (ce instanceof IsDeadConnectionException || ce instanceof IsTimeoutException)) {
                    closeConnection(connection);
                } else if (!this.idleConnections.offer(connection)) {
                    closeConnection(connection);
                } else {
                    this.monitor.incConnectionReturned(connection
                            .getHostConnectionPool().getHost());
                }
            }
            return true;
        }
        return false;
    }

    private void closeConnection(Connection<CL> connection) {
        connection.getHostConnectionPool().closeConnection(connection);
        activeConnectionCount.decrementAndGet();
    }

    class BagExecuteWithFailover<R> extends
            AbstractExecuteWithFailoverImpl<CL, R> {
        private int retryCountdown;
        private HostConnectionPool<CL> pool = null;
        private int size = 0;

        public BagExecuteWithFailover(ConnectionPoolConfiguration config)
                throws ConnectionException {
            super(config, monitor);

            size = topology.getAllPools().getPools().size();
            retryCountdown = Math.min(config.getMaxFailoverCount(), size);
            if (retryCountdown < 0)
                retryCountdown = size;
        }

        @Override
        public HostConnectionPool<CL> getCurrentHostConnectionPool() {
            return pool;
        }

        @Override
        public Connection<CL> borrowConnection(Operation<CL, R> operation)
                throws ConnectionException {
            pool = null;
            connection = BagOfConnectionsConnectionPoolImpl.this
                    .borrowConnection(operation);
            pool = connection.getHostConnectionPool();
            return connection;
        }

        @Override
        public boolean canRetry() {
            return --retryCountdown >= 0;
        }

        @Override
        public void releaseConnection() {
            BagOfConnectionsConnectionPoolImpl.this
                    .returnConnection(connection);
            connection = null;
        }

    }

    @Override
    public <R> ExecuteWithFailover<CL, R> newExecuteWithFailover(
            Operation<CL, R> op) throws ConnectionException {
        return new BagExecuteWithFailover<R>(config);
    }
}
