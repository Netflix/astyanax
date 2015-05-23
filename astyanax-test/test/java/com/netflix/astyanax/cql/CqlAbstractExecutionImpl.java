package com.netflix.astyanax.cql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationCategory;
import com.netflix.astyanax.CassandraOperationTracer;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.Execution;
import com.netflix.astyanax.KeyspaceTracerFactory;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.IsRetryableException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.retrypolicies.JavaDriverBasedRetryPolicy;
import com.netflix.astyanax.cql.util.AsyncOperationResult;
import com.netflix.astyanax.cql.util.CFQueryContext;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;

/**
 * Abstract class that encapsulates the functionality for executing an operations using the native protocol based java driver
 * Note that class provides only the operation agnostic functionality such as retries, tracking metrics etc. 
 * The actual logic for constructing the query for the operation and then parsing the result set of the operation is left
 * to the implementation of the extending class. 
 * 
 * @author poberai
 *
 * @param <R>
 */
public abstract class CqlAbstractExecutionImpl<R> implements Execution<R> {
	
	private static final Logger LOG = LoggerFactory.getLogger(CqlAbstractExecutionImpl.class);
	
	// The session for executing the query
	protected final Session session;
	// The keyspace being operated on
	protected final String keyspace;
	// The CF being operated on
	protected final ColumnFamily<?, ?> cf;
	// Factory for vending operation metrics
	protected final KeyspaceTracerFactory tracerFactory;
	// Retry policy
	protected final RetryPolicy retry;
	// ConsistencyLevel
	protected final com.datastax.driver.core.ConsistencyLevel clLevel; 
	
	public CqlAbstractExecutionImpl(KeyspaceContext ksContext, CFQueryContext<?,?> cfContext) {
		
		this.session = ksContext.getSession();
		this.keyspace = ksContext.getKeyspace();
		this.cf = (cfContext != null) ? cfContext.getColumnFamily() : null;
		this.tracerFactory = ksContext.getTracerFactory();
		
		// process the override retry policy first
		RetryPolicy retryPolicy = ksContext.getConfig().getRetryPolicy();
		retry = (retryPolicy != null) ? retryPolicy : getRetryPolicy(cfContext.getRetryPolicy()); 
		
		clLevel = resolveConsistencyLevel(ksContext, cfContext);
	}

	public CqlAbstractExecutionImpl(KeyspaceContext ksContext, RetryPolicy retryPolicy) {
		
		this.session = ksContext.getSession();
		this.keyspace = ksContext.getKeyspace();
		this.cf = null;
		this.tracerFactory = ksContext.getTracerFactory();
		
		// process the override retry policy first
		retry = (retryPolicy != null) ? retryPolicy : getRetryPolicy(ksContext.getConfig().getRetryPolicy());
		clLevel = resolveConsistencyLevel(ksContext, null);
	}

	@Override
	public OperationResult<R> execute() throws ConnectionException {
		
        ConnectionException lastException = null;
        
        retry.begin();

        do {
        	try {
                return executeOp();
            } catch (RuntimeException ex) {
            	lastException = new OperationException(ex);
            } catch (ConnectionException ex) {
                if (ex instanceof IsRetryableException)
                    lastException = ex;
                else
                    throw ex;
            }
        } while (retry.allowRetry());

        throw lastException;
	}

	
	private OperationResult<R> executeOp() throws ConnectionException {
		CassandraOperationTracer tracer = null;
		
		if (cf != null) {
			tracer = tracerFactory.newTracer(getOperationType(), cf);
		} else {
			tracer = tracerFactory.newTracer(getOperationType());
		}
		
		tracer.start();
		Statement query = getQuery();
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Query: " + query);
		}
		
        // Set the consistency level on the query
        query.setConsistencyLevel(clLevel);

        // Set the retry policy on the query
        if (retry instanceof JavaDriverBasedRetryPolicy) {
        	JavaDriverBasedRetryPolicy jdRetryPolicy = (JavaDriverBasedRetryPolicy) retry;
        	query.setRetryPolicy(jdRetryPolicy.getJDRetryPolicy());
        }

        ResultSet resultSet = session.execute(query);
        R result = parseResultSet(resultSet);
		OperationResult<R> opResult = new CqlOperationResultImpl<R>(resultSet, result);
		opResult.setAttemptsCount(retry.getAttemptCount());
		tracer.success();
		return opResult;
	}
	
	@Override
	public ListenableFuture<OperationResult<R>> executeAsync() throws ConnectionException {
		final CassandraOperationTracer tracer = tracerFactory.newTracer(getOperationType());
		tracer.start();
		
		Statement query = getQuery();
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Query: " + query);
		}
		
		ResultSetFuture rsFuture = session.executeAsync(query);
		return new AsyncOperationResult<R>(rsFuture) {
			@Override
			public OperationResult<R> getOperationResult(ResultSet resultSet) {
				R result = null;
				try {
					result = parseResultSet(resultSet);
				} catch (NotFoundException e) {
					e.printStackTrace();
				}
				tracer.success();
				OperationResult<R> opResult = new CqlOperationResultImpl<R>(resultSet, result);
				opResult.setAttemptsCount(retry.getAttemptCount());
				return opResult;
			}
		};
	}
	
	private RetryPolicy getRetryPolicy(RetryPolicy policy) {
		if (policy != null) {
			return policy.duplicate();
		} else {
			return null;
		}
	}
	
	private ConsistencyLevel getDefaultCL(KeyspaceContext ksContext) {
		
		ConsistencyLevel clLevel = ksContext.getConfig().getDefaultReadConsistencyLevel(); 
		
		CassandraOperationCategory op = getOperationType().getCategory();
		switch (op) {
		case READ:
			clLevel = ksContext.getConfig().getDefaultReadConsistencyLevel(); 
			break;
		case WRITE:
			clLevel = ksContext.getConfig().getDefaultWriteConsistencyLevel();
		default:
			clLevel = ksContext.getConfig().getDefaultReadConsistencyLevel(); 
		}
		
		return clLevel;
	}
	
	private com.datastax.driver.core.ConsistencyLevel resolveConsistencyLevel(KeyspaceContext ksContext, CFQueryContext<?,?> cfContext) {
		ConsistencyLevel clLevel = null; 
		if (cfContext != null) {
			clLevel = cfContext.getConsistencyLevel();
		}
		if (clLevel == null) {
			clLevel = getDefaultCL(ksContext);
		}
		return ConsistencyLevelMapping.getCL(clLevel);
	}
	
	/**
	 * Specify what operation type this is. Used for emitting the right tracers
	 * @return CassandraOperationType
	 */
	public abstract CassandraOperationType getOperationType();
	
	/**
	 * Get the Query for this operation
	 * @return Query
	 */
	public abstract Statement getQuery();

	/**
	 * Parse the result set to get the required response
	 * @param resultSet
	 * @return
	 */
	public abstract R parseResultSet(ResultSet resultSet) throws NotFoundException;
}
