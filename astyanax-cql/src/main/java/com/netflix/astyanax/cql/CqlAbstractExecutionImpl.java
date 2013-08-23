package com.netflix.astyanax.cql;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationTracer;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.Execution;
import com.netflix.astyanax.KeyspaceTracerFactory;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.util.AsyncOperationResult;
import com.netflix.astyanax.cql.util.ChainedContext;
import com.netflix.astyanax.model.ColumnFamily;

public abstract class CqlAbstractExecutionImpl<R> implements Execution<R> {
	
	private final Cluster cluster; 
	private final ColumnFamily<?, ?> cf;
	private final ChainedContext context; 
	private final KeyspaceTracerFactory tracerFactory;
	
	public CqlAbstractExecutionImpl(ChainedContext context) {
		this.context = context.clone();
		
		this.context.rewindForRead();
		this.cluster = context.getNext(Cluster.class);
		
		// note that this may be null
		@SuppressWarnings("unused")
		String keyspace = context.getNext(String.class);
		cf = context.getNext(ColumnFamily.class);
		
		this.tracerFactory = context.getTracerFactory();
	}

	@Override
	public OperationResult<R> execute() throws ConnectionException {
		
		CassandraOperationTracer tracer = null;
		
		if (cf != null) {
			tracer = tracerFactory.newTracer(getOperationType(), cf);
		} else {
			tracer = tracerFactory.newTracer(getOperationType());
		}
		
		tracer.start();
		ResultSet resultSet = cluster.connect().execute(getQuery());
		R result = parseResultSet(resultSet);
		OperationResult<R> opResult = new CqlOperationResultImpl<R>(resultSet, result);
		tracer.success();
		return opResult;
	}

	@Override
	public ListenableFuture<OperationResult<R>> executeAsync() throws ConnectionException {
		final CassandraOperationTracer tracer = tracerFactory.newTracer(getOperationType());
		tracer.start();
		ResultSetFuture rsFuture = cluster.connect().executeAsync(getQuery());
		return new AsyncOperationResult<R>(rsFuture) {
			@Override
			public OperationResult<R> getOperationResult(ResultSet resultSet) {
				R result = parseResultSet(resultSet);
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
	public abstract R parseResultSet(ResultSet resultSet);
}
