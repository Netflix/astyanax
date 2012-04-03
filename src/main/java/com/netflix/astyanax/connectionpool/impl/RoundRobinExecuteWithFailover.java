package com.netflix.astyanax.connectionpool.impl;

import java.util.List;

import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NoAvailableHostsException;

public class RoundRobinExecuteWithFailover<CL, R> extends
        AbstractExecuteWithFailoverImpl<CL, R> {
    private int index;
    protected HostConnectionPool<CL> pool;
    private int retryCountdown;
    protected final List<HostConnectionPool<CL>> pools;
    protected int size;
    protected int waitDelta;
    protected int waitMultiplier = 1;

    public RoundRobinExecuteWithFailover(ConnectionPoolConfiguration config,
            ConnectionPoolMonitor monitor, List<HostConnectionPool<CL>> pools,
            int index) throws ConnectionException {
        super(config, monitor);

        this.index = index;
        this.pools = pools;

        if (pools == null || pools.isEmpty()) {
            monitor.incNoHosts();
            throw new NoAvailableHostsException("No hosts to borrow from");
        }

        size = pools.size();
        retryCountdown = Math.min(config.getMaxFailoverCount(), size);
        if (retryCountdown < 0)
            retryCountdown = size;
        else if (retryCountdown == 0)
            retryCountdown = 1;

        waitDelta = config.getMaxTimeoutWhenExhausted() / retryCountdown;
    }

    public int getNextHostIndex() {
        return index++ % size;
    }

    public boolean canRetry() {
        return --retryCountdown > 0;
    }

    @Override
    public HostConnectionPool<CL> getCurrentHostConnectionPool() {
        return pool;
    }

    @Override
    public Connection<CL> borrowConnection(Operation<CL, R> operation)
            throws ConnectionException {
        pool = pools.get(getNextHostIndex());
        return pool.borrowConnection(waitDelta * waitMultiplier);
    }

}
