package com.netflix.astyanax.cql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.ListenableFuture;
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
import com.netflix.astyanax.cql.writes.CqlColumnListMutationImpl.ColumnFamilyMutationContext;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.retry.RetryPolicy;

public abstract class CqlAbstractExecutionImpl<R> implements Execution<R> {
	
	private static final Logger LOG = LoggerFactory.getLogger(CqlAbstractExecutionImpl.class);
	
	protected final Session session;
	protected final String keyspace;
	protected final ColumnFamily<?, ?> cf;
	protected final KeyspaceTracerFactory tracerFactory;
	protected final RetryPolicy retry;
	
	public CqlAbstractExecutionImpl(KeyspaceContext ksContext, ColumnFamilyMutationContext<?,?> cfContext) {
		
		this.session = ksContext.getSession();
		this.keyspace = ksContext.getKeyspace();
		this.cf = (cfContext != null) ? cfContext.getColumnFamily() : null;
		this.tracerFactory = ksContext.getTracerFactory();
		
		// process the override retry policy first
		retry = (cfContext != null && cfContext.getRetryPolicy() != null) ? 
				cfContext.getRetryPolicy() : ksContext.getConfig().getRetryPolicyFactory().createRetryPolicy();
	}

	public CqlAbstractExecutionImpl(KeyspaceContext ksContext, RetryPolicy retryPolicy) {
		
		this.session = ksContext.getSession();
		this.keyspace = ksContext.getKeyspace();
		this.cf = null;
		this.tracerFactory = ksContext.getTracerFactory();
		
		// process the override retry policy first
		retry = (retryPolicy != null) ? retryPolicy : ksContext.getConfig().getRetryPolicyFactory().createRetryPolicy();
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
		Query query = getQuery();
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Query: " + query);
		}
		
        if (retry instanceof JavaDriverBasedRetryPolicy) {
        	JavaDriverBasedRetryPolicy jdRetryPolicy = (JavaDriverBasedRetryPolicy) retry;
        	query.setRetryPolicy(jdRetryPolicy.getJDRetryPolicy());
        }

		ResultSet resultSet = session.execute(query);
		R result = parseResultSet(resultSet);
		OperationResult<R> opResult = new CqlOperationResultImpl<R>(resultSet, result);
		tracer.success();
		return opResult;
	}
	
	@Override
	public ListenableFuture<OperationResult<R>> executeAsync() throws ConnectionException {
		final CassandraOperationTracer tracer = tracerFactory.newTracer(getOperationType());
		tracer.start();
		
		Query query = getQuery();
		
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
				return new CqlOperationResultImpl<R>(resultSet, result);
			}
		};
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
	public abstract Query getQuery();

	/**
	 * Parse the result set to get the required response
	 * @param resultSet
	 * @return
	 */
	public abstract R parseResultSet(ResultSet resultSet) throws NotFoundException;
}
