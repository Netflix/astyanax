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

import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.ConnectionPool;
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

/**
 * Abstract class that provides a basic impl of {@link ExecuteWithFailover}. 
 * 
 * This is used within the context of a {@link AbstractHostPartitionConnectionPool} implementation, where 
 * the abstract failover class repeatedly attempts to borrow a connection from the implementing {@link ConnectionPool}
 * and also releases the connection as cleanup. It also maintains stats about the attempts and informs latency metrics 
 * on the exceptions when executing {@link Operation}s or borrowing {@link Connection}s from the pool. <br/> <br/>
 * 
 * Note that extending classes need to implement functionality to {@link AbstractExecuteWithFailoverImpl#borrowConnection(Operation)} that must be used
 * to execute the operation. They also need to implement {@link AbstractExecuteWithFailoverImpl#canRetry()} to tell this class 
 * when to stop borrowing connections on failed attempts. 
 *  
 * @param <CL>
 * @param <R>
 * 
 * @author elandau
 *
 * @see {@link AbstractHostPartitionConnectionPool#executeWithFailover(Operation, com.netflix.astyanax.retry.RetryPolicy)} for references to this.
 * @see {@link RoundRobinExecuteWithFailover} as an example class that extends the functionality. 
 * @see {@link RoundRobinConnectionPoolImpl} as an example of a {@link ConnectionPool} that employs the {@link RoundRobinExecuteWithFailover} for it's failover impl
 * 
 */
public abstract class AbstractExecuteWithFailoverImpl<CL, R> implements ExecuteWithFailover<CL, R> {
    protected Connection<CL> connection = null;
    private long startTime;
    private long poolStartTime;
    private int attemptCounter = 0;
    private final ConnectionPoolMonitor monitor;
    protected final ConnectionPoolConfiguration config;
    
    /**
     * Public constructor
     * @param config
     * @param monitor
     * @throws ConnectionException
     */
    public AbstractExecuteWithFailoverImpl(ConnectionPoolConfiguration config, ConnectionPoolMonitor monitor)
            throws ConnectionException {
    	this.monitor = monitor;
    	this.config = config;
        startTime = poolStartTime = System.currentTimeMillis();
    }
    
    /**
     * @return {@link Host}
     */
	final public Host getCurrentHost() {
		HostConnectionPool<CL> pool = getCurrentHostConnectionPool();
		if (pool != null)
			return pool.getHost();
		else 
			return Host.NO_HOST;
	}
	
	/**
	 * @return {@link HostConnectionPool}
	 */
	abstract public HostConnectionPool<CL> getCurrentHostConnectionPool();

	/**
	 * @param operation
	 * @return {@link Connection}
	 * @throws ConnectionException
	 */
    abstract public Connection<CL> borrowConnection(Operation<CL, R> operation) throws ConnectionException;

    /**
     * @return boolean
     */
	abstract public boolean canRetry();
	
	/**
	 * Basic impl that repeatedly borrows a conn and tries to execute the operation while maintaining metrics for 
	 * success, conn attempts, failures and latencies for operation executions
	 * 
	 * @param operation
	 * @return {@link OperationResult}
	 */
	@Override
	public OperationResult<R> tryOperation(Operation<CL, R> operation) throws ConnectionException {
	    Operation<CL, R> filteredOperation = config.getOperationFilterFactory().attachFilter(operation);
	    
        while (true) {
            attemptCounter++;
            
            try {
                connection = borrowConnection(filteredOperation);
                startTime = System.currentTimeMillis();
                OperationResult<R> result = connection.execute(filteredOperation);
                result.setAttemptsCount(attemptCounter);
                monitor.incOperationSuccess(getCurrentHost(), result.getLatency());
                return result;
            }
            catch (Exception e) {
                ConnectionException ce = (e instanceof ConnectionException) ? (ConnectionException) e
                        : new UnknownException(e);
            	try {
            		informException(ce);
                    monitor.incFailover(ce.getHost(), ce);
            	}
            	catch (ConnectionException ex) {
                    monitor.incOperationFailure(getCurrentHost(), ex);
                    throw ex;
            	}
            }
            finally {
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
    
    private void informException(ConnectionException connectionException) throws ConnectionException {
        connectionException
            .setHost(getCurrentHost())
        	.setLatency(System.currentTimeMillis() - startTime)
        	.setAttempt(this.attemptCounter)
        	.setLatencyWithPool(System.currentTimeMillis() - poolStartTime);

        if (connectionException instanceof IsRetryableException) {
            if (!canRetry()) {
                throw connectionException;
            }
        }
        else {
            // Most likely an operation error
            throw connectionException;
        }
    }		
}
