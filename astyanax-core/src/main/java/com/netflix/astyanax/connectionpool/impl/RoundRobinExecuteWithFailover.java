package com.netflix.astyanax.connectionpool.impl;

import java.util.List;

import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NoAvailableHostsException;

/**
 * Class that extends {@link AbstractExecuteWithFailoverImpl} to provide functionality for borrowing a {@link Connection} from a list of {@link HostConnectionPool}(s)
 * in a round robin fashion. <br/> <br/>
 * 
 * It maintains state of the current and next pool to be used by a revolving index over the list of pools, hence round robin.
 * It also maintains state of how many retries have been done for this instance and consults the 
 * {@link ConnectionPoolConfiguration#getMaxFailoverCount()} threshold.
 *  
 * @author elandau
 *
 * @param <CL>
 * @param <R>
 * 
 * @see {@link AbstractExecuteWithFailoverImpl} for details on how failover is repeatedly called for ensuring that an {@link Operation} can be executed with resiliency.
 * @see {@link RoundRobinConnectionPoolImpl} for the impl that references this class. 
 * @see {@link AbstractHostPartitionConnectionPool} for more context on how failover functionality is used within the context of an operation execution 
 * 
 */
public class RoundRobinExecuteWithFailover<CL, R> extends AbstractExecuteWithFailoverImpl<CL, R> {
    private int index;
    protected HostConnectionPool<CL> pool;
    private int retryCountdown;
    protected final List<HostConnectionPool<CL>> pools;
    protected final int size;
    protected int waitDelta;
    protected int waitMultiplier = 1;

    public RoundRobinExecuteWithFailover(ConnectionPoolConfiguration config, ConnectionPoolMonitor monitor,
            List<HostConnectionPool<CL>> pools, int index) throws ConnectionException {
        super(config, monitor);

        this.index = index;
        this.pools = pools;

        if (pools == null || pools.isEmpty()) {
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
        try {
            return index % size;
        }
        finally {
            index++;
            if (index < 0)
                index = 0;
        }
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
        pool = pools.get(getNextHostIndex());
        return pool.borrowConnection(waitDelta * waitMultiplier);
    }

}
