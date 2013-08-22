package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlOperationResultImpl;
import com.netflix.astyanax.cql.util.AsyncOperationResult;
import com.netflix.astyanax.cql.util.ChainedContext;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.query.ColumnCountQuery;

public class CqlColumnCountQueryImpl implements ColumnCountQuery {

	private ChainedContext context; 
	
	public CqlColumnCountQueryImpl(ChainedContext ctx) {
		this.context = ctx;
	}
	
	@Override
	public OperationResult<Integer> execute() throws ConnectionException {
		
		context.rewindForRead();
		Cluster cluster = context.getNext(Cluster.class);
		return parseResponse(cluster.connect().execute(getQuery()));
	}

	@Override
	public ListenableFuture<OperationResult<Integer>> executeAsync() throws ConnectionException {
		
		context.rewindForRead();
		Cluster cluster = context.getNext(Cluster.class);

		ResultSetFuture rsFuture = cluster.connect().executeAsync(getQuery());
		
		return new AsyncOperationResult<Integer>(rsFuture) {
			@Override
			public OperationResult<Integer> getOperationResult(ResultSet rs) {
				return parseResponse(rs);
			}
		};
	}

	private Query getQuery() {

		String keyspace = context.getNext(String.class);
		ColumnFamily<?, ?> cf = context.getNext(ColumnFamily.class);
		Object rowKey = context.getNext(Object.class); 
		
		Query query = QueryBuilder.select().countAll()
				  .from(keyspace, cf.getName())
				  .where(eq(cf.getKeyAlias(), rowKey));
		
		return query;
	}
	
	private OperationResult<Integer> parseResponse(ResultSet rs) {
		Long count = rs.one().getLong(0);
		return new CqlOperationResultImpl<Integer>(rs, count.intValue());
	}
}
