package com.netflix.astyanax.connectionpool.impl;

import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.ExecuteWithFailover;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.IsRetryableException;
import com.netflix.astyanax.connectionpool.exceptions.UnknownException;

public abstract class AbstractExecuteWithFailoverImpl<CL, R> implements
        ExecuteWithFailover<CL, R> {
    protected Connection<CL> connection = null;
    private long startTime;
    private long poolStartTime;
    private int attemptCounter = 0;
    private final ConnectionPoolMonitor monitor;
    protected final ConnectionPoolConfiguration config;

    public AbstractExecuteWithFailoverImpl(ConnectionPoolConfiguration config,
            ConnectionPoolMonitor monitor) throws ConnectionException {
        this.monitor = monitor;
        this.config = config;
        startTime = poolStartTime = System.currentTimeMillis();
    }

    final public Host getCurrentHost() {
        HostConnectionPool<CL> pool = getCurrentHostConnectionPool();
        if (pool != null)
            return pool.getHost();
        else
            return Host.NO_HOST;
    }

    abstract public HostConnectionPool<CL> getCurrentHostConnectionPool();

    abstract public Connection<CL> borrowConnection(Operation<CL, R> operation)
            throws ConnectionException;

    abstract public boolean canRetry();

    @Override
    public OperationResult<R> tryOperation(Operation<CL, R> operation)
            throws ConnectionException {
        while (true) {
            attemptCounter++;

            try {
                connection = borrowConnection(operation);
                startTime = System.currentTimeMillis();
                OperationResult<R> result = connection.execute(operation);
                result.setAttemptsCount(attemptCounter);
                monitor.incOperationSuccess(getCurrentHost(),
                        result.getLatency());
                return result;
            } catch (Exception e) {
                ConnectionException ce = (e instanceof ConnectionException) ? (ConnectionException) e
                        : new UnknownException(e);
                try {
                    informException(ce);
                } catch (ConnectionException ex) {
                    monitor.incOperationFailure(getCurrentHost(), ex);
                    throw ex;
                }
            } finally {
                releaseConnection();
            }
        }
    }

    protected void releaseConnection() {
        if (connection != null) {
            connection.getHostConnectionPool().returnConnection(connection);
            connection = null;
        }
    }

    public void informException(ConnectionException connectionException)
            throws ConnectionException {
        connectionException.setHost(getCurrentHost())
                .setLatency(System.currentTimeMillis() - startTime)
                .setAttempt(this.attemptCounter)
                .setLatencyWithPool(System.currentTimeMillis() - poolStartTime);

        if (connectionException instanceof IsRetryableException) {
            if (!canRetry()) {
                throw connectionException;
            }

            monitor.incFailover(connectionException.getHost(),
                    connectionException);
        } else {
            // Most likely an operation error
            throw connectionException;
        }
    }
}
