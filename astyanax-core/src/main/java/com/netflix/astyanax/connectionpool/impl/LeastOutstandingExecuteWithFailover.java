package com.netflix.astyanax.connectionpool.impl;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NoAvailableHostsException;

/**
 * Finds the {@link HostConnectionPool} with the max idle connections when borrowing a connection from the pool. 
 * This execute with failover impl is used within the {@link TokenAwareConnectionPoolImpl} class depending on the configured 
 * {@link HostSelectorStrategy}
 *  
 * @author elandau
 *
 * @param <CL>
 * @param <R>
 * 
 * @see {@link TokenAwareConnectionPoolImpl#executeWithFailover(Operation, com.netflix.astyanax.retry.RetryPolicy)} for details on where this class is referenced
 */
public class LeastOutstandingExecuteWithFailover<CL, R> extends AbstractExecuteWithFailoverImpl<CL, R> {
    protected HostConnectionPool<CL> pool;
    private int retryCountdown;
    protected final List<HostConnectionPool<CL>> pools;
    protected int waitDelta;
    protected int waitMultiplier = 1;

    public LeastOutstandingExecuteWithFailover(ConnectionPoolConfiguration config, ConnectionPoolMonitor monitor,
                                         List<HostConnectionPool<CL>> pools) throws ConnectionException {
        super(config, monitor);

        this.pools = Lists.newArrayList(pools);

        if (this.pools == null || this.pools.isEmpty()) {
            throw new NoAvailableHostsException("No hosts to borrow from");
        }

        int size = this.pools.size();
        retryCountdown = Math.min(config.getMaxFailoverCount(), size);
        if (retryCountdown < 0)
            retryCountdown = size;
        else if (retryCountdown == 0)
            retryCountdown = 1;

        waitDelta = config.getMaxTimeoutWhenExhausted() / retryCountdown;
    }

    public boolean canRetry() {
        return --retryCountdown > 0;
    }

    @Override
    public HostConnectionPool<CL> getCurrentHostConnectionPool() {
        return pool;
    }

    @Override
    public Connection<CL> borrowConnection(Operation<CL, R> operation) throws ConnectionException {
        // find the pool with the least outstanding (i.e most idle) active connections
        Iterator<HostConnectionPool<CL>> iterator = this.pools.iterator();
        HostConnectionPool eligible = iterator.next();
        while (iterator.hasNext()) {
            HostConnectionPool<CL> candidate = iterator.next();
            if (candidate.getIdleConnectionCount() > eligible.getIdleConnectionCount()) {
                eligible = candidate;
            }
        }
        return eligible.borrowConnection(waitDelta * waitMultiplier);
    }

}
