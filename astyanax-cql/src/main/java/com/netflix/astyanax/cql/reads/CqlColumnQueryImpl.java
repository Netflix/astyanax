package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlOperationResultImpl;
import com.netflix.astyanax.cql.util.AsyncOperationResult;
import com.netflix.astyanax.cql.util.ChainedContext;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.query.ColumnQuery;

public class CqlColumnQueryImpl<C> implements ColumnQuery<C> {

	private ChainedContext context; 
	
	CqlColumnQueryImpl(ChainedContext ctx) {
		this.context = ctx;
	}
	
	@Override
	public OperationResult<Column<C>> execute() throws ConnectionException {
		
		context.rewindForRead();
		Cluster cluster = context.getNext(Cluster.class);
		return parseResponse(cluster.connect().execute(getQuery()));
	}

	@Override
	public ListenableFuture<OperationResult<Column<C>>> executeAsync() throws ConnectionException {

		context.rewindForRead();
		Cluster cluster = context.getNext(Cluster.class);

		ResultSetFuture rsFuture = cluster.connect().executeAsync(getQuery());
		
		return new AsyncOperationResult<Column<C>>(rsFuture) {
			@Override
			public OperationResult<Column<C>> getOperationResult(ResultSet rs) {
				return parseResponse(rs);
			}
		};
	}

	private Query getQuery() {
		String keyspace = context.getNext(String.class);
		ColumnFamily<?, ?> cf = context.getNext(ColumnFamily.class);
		Object rowKey = context.getNext(Object.class); 
		Object column = context.getNext(Object.class); 
		
		Query query = QueryBuilder.select(String.valueOf(column))
				  .from(keyspace, cf.getName())
				  .where(eq("key", rowKey));
		return query;
	}

	private OperationResult<Column<C>> parseResponse(ResultSet rs) {
		Row row = rs.one();
		String columnName = rs.getColumnDefinitions().asList().get(0).getName();
		return new CqlOperationResultImpl<Column<C>>(rs, new CqlColumnImpl<C>(columnName, row, 0));
	}
}
