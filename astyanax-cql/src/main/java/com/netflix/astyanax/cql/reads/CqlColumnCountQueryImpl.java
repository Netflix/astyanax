package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlAbstractExecutionImpl;
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
		return new InternalColumnCountExecutionImpl().execute();
	}

	@Override
	public ListenableFuture<OperationResult<Integer>> executeAsync() throws ConnectionException {
		return new InternalColumnCountExecutionImpl().executeAsync();
	}

	private class InternalColumnCountExecutionImpl extends CqlAbstractExecutionImpl<Integer> {

		public InternalColumnCountExecutionImpl() {
			super(context);
		}

		@Override
		public CassandraOperationType getOperationType() {
			return CassandraOperationType.GET_COLUMN_COUNT;
		}

		@Override
		public Query getQuery() {
			String keyspace = context.getNext(String.class);
			ColumnFamily<?, ?> cf = context.getNext(ColumnFamily.class);
			Object rowKey = context.getNext(Object.class); 
			
			Query query = QueryBuilder.select().countAll()
					  .from(keyspace, cf.getName())
					  .where(eq(cf.getKeyAlias(), rowKey));
			
			return query;
		}

		@Override
		public Integer parseResultSet(ResultSet resultSet) {
			Long count = resultSet.one().getLong(0);
			return count.intValue();
		}
	}
}
