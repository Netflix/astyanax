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
import com.netflix.astyanax.cql.CqlFamilyFactory;
import com.netflix.astyanax.cql.util.ChainedContext;
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

		private final Object rowKey;
		
		public InternalColumnCountExecutionImpl() {
			super(context);
			rowKey = context.getNext(Object.class);
		}

		@Override
		public CassandraOperationType getOperationType() {
			return CassandraOperationType.GET_COLUMN_COUNT;
		}

		@Override
		public Query getQuery() {
			
			if (CqlFamilyFactory.OldStyleThriftMode()) {
				return QueryBuilder.select("column1")
						.from(keyspace, cf.getName())
						.where(eq(cf.getKeyAlias(), rowKey));
			} else {
				return QueryBuilder.select().countAll()
						.from(keyspace, cf.getName())
						.where(eq(cf.getKeyAlias(), rowKey));
			}
		}

		@Override
		public Integer parseResultSet(ResultSet resultSet) {
			if (CqlFamilyFactory.OldStyleThriftMode()) {
				return resultSet.all().size();
			} else {
				Long count = resultSet.one().getLong(0);
				return count.intValue();
			}
		}
	}
}
