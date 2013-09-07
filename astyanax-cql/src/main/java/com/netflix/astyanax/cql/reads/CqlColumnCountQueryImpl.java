package com.netflix.astyanax.cql.reads;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlAbstractExecutionImpl;
import com.netflix.astyanax.cql.CqlFamilyFactory;
import com.netflix.astyanax.cql.util.ChainedContext;
import com.netflix.astyanax.query.ColumnCountQuery;

public class CqlColumnCountQueryImpl implements ColumnCountQuery {

	private final ChainedContext context; 
	private final Query query;
	
	public CqlColumnCountQueryImpl(ChainedContext ctx, Query query) {
		this.context = ctx;
		this.query = query;
	}
	
	@Override
	public OperationResult<Integer> execute() throws ConnectionException {
		return new InternalColumnCountExecutionImpl(query).execute();
	}

	@Override
	public ListenableFuture<OperationResult<Integer>> executeAsync() throws ConnectionException {
		return new InternalColumnCountExecutionImpl(query).executeAsync();
	}

	private class InternalColumnCountExecutionImpl extends CqlAbstractExecutionImpl<Integer> {

		private final Query query;
		
		public InternalColumnCountExecutionImpl(Query query) {
			super(context);
			this.query = query;
		}

		@Override
		public CassandraOperationType getOperationType() {
			return CassandraOperationType.GET_COLUMN_COUNT;
		}

		@Override
		public Query getQuery() {
			return this.query;
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
