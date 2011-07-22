package com.netflix.astyanax.connectionpool.impl;

import com.netflix.astyanax.connectionpool.*;
import com.netflix.astyanax.connectionpool.exceptions.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class RoundRobinConnectionPoolImpl<CL> extends AbstractHostPartitionConnectionPool<CL> {

	private final AtomicInteger current = new AtomicInteger(0);
	private final AtomicReference<List<HostConnectionPool<CL>>> poolsRef = new
		AtomicReference<List<HostConnectionPool<CL>>>();
	
	public RoundRobinConnectionPoolImpl(ConnectionPoolConfiguration config, 
			ConnectionFactory<CL> factory) {
		super(config, factory);
	}
	
	@Override
	public <R> Connection<CL> borrowConnection(Operation<CL, R> op)
			throws ConnectionException, OperationException {
		throw new UnsupportedOperationException("Coming soon");
	}

    public <R> ExecuteWithFailover<CL, R>   newExecuteWithFailover() throws ConnectionException {
        return new ExecuteWithFailover<CL, R>() {
            private final List<HostConnectionPool<CL>> pools = poolsRef.get();
            private final int size = pools.size();
            private int retryCount;
            private int index;
            private boolean isDone = false;
            private boolean doFailover = false;
            private HostConnectionPool<CL> pool;

            // Constructor
            {
                if (pools.isEmpty()) {
                	monitor.incNoHosts();
                	isDone = true;
                    throw new NoAvailableHostsException("No hosts to borrow from");
                }

                index = current.incrementAndGet()%size;
                
                retryCount = Math.min(failoverStrategy.getMaxRetries(), size);
                if (retryCount < 0)
                	retryCount = size;
            }

            public Host getHost() {
                return (pool != null) ? pool.getHost() : Host.NO_HOST;
            }

            @Override
            public OperationResult<R> tryOperation(Operation<CL, R> operation) throws ConnectionException {
                while ( !isDone ) {
                    if ( doFailover ) {
                        doFailover = false;

                        monitor.incFailover();
                        if (failoverStrategy.getWaitTime() > 0) {
                            try {
                                Thread.sleep(failoverStrategy.getWaitTime());
                            }
                            catch (InterruptedException e) {
                                isDone = true;
                                Thread.currentThread().interrupt();
                                throw new TimeoutException("Interrupted sleeping between retries");
                            }
                        }
                    }
                    
                    // 1. Select the next pool
                    pool = pools.get(index++);

                    // 2. Try to get a connection
                    Connection<CL> connection = null;
                    try {
                        long startTime = System.currentTimeMillis();
                        connection = pool.borrowConnection(exhaustedStrategy.getWaitTime());
                        
                        monitor.incConnectionBorrowed(pool.getHost(), System.currentTimeMillis() - startTime);

                        // 3. Now try to execute
                        OperationResult<R> result = connection.execute(operation);
                        monitor.incOperationSuccess(pool.getHost(), result.getLatency());
                        return result;
                    }
                    catch (ConnectionException e) {
                        informException(e);
                    }
                    finally {
                        if ( connection != null ) {
                            returnConnection(connection);
                        }
                    }

                }
                throw new TimeoutException("Operation failed too many times");
            }

            @Override
            public void informException(ConnectionException connectionException) throws ConnectionException {
                monitor.incOperationFailure(getHost(), connectionException);
                
                if (   connectionException instanceof TimeoutException 
                	|| connectionException instanceof TransportException 
                	|| connectionException instanceof UnknownException) {
                    if (badHostDetector.checkFailure(getHost(), connectionException)) {
                        markHostAsDown(pool, connectionException);
                    }
                }
                
            	// Got a ConnectionException.  This could be either an application
            	// error or an error getting a connection from the connection pool
                // Have a connection but failed to execute it
                if (!connectionException.isRetryable()) {
                    isDone = true;
                    throw connectionException;
                }

                doFailover = --retryCount > 0;
                if (false == doFailover) {
                    isDone = true;
                    throw new PoolTimeoutException("Timed out trying to borrow a connection.  " + connectionException.getMessage());
                }
            }
        };
    }

	@Override
	public <R> OperationResult<R> executeWithFailover(Operation<CL, R> op)
			throws ConnectionException {
        ExecuteWithFailover<CL, R> withFailover = newExecuteWithFailover();
        return withFailover.tryOperation(op);
    }
	
	@Override 
	protected void onHostDown(HostConnectionPool<CL> pool) {
		rebuildPools();
	}
	
	@Override 
	protected void onHostUp(HostConnectionPool<CL> pool) {
		rebuildPools();
	}

	private void rebuildPools() {
		Collection<HostConnectionPool<CL>> hosts = activeHosts.values();
		if (hosts != null && !hosts.isEmpty()) {
			this.poolsRef.set(new ArrayList<HostConnectionPool<CL>>(hosts));
		}
		else {
			this.poolsRef.set(new ArrayList<HostConnectionPool<CL>>());
		}
	}
}
