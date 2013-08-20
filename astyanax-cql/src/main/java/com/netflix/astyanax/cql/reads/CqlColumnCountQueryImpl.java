package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlOperationResultImpl;
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
		String keyspace = context.getNext(String.class);
		ColumnFamily<?, ?> cf = context.getNext(ColumnFamily.class);
		Object rowKey = context.getNext(Object.class); 
		
		// TODO: need to handle for any column range query. 
		
		Query query = QueryBuilder.select().countAll()
				  .from(keyspace, cf.getName())
				  .where(eq(cf.getKeyAlias(), rowKey));
		
		ResultSet rs = cluster.connect().execute(query);
		Long count = rs.one().getLong(0);
		return new CqlOperationResultImpl<Integer>(rs, count.intValue());
	}

	@Override
	public ListenableFuture<OperationResult<Integer>> executeAsync() throws ConnectionException {
		throw new NotImplementedException();
	}
}
