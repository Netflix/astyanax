package com.netflix.astyanax.cql.reads;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlAbstractExecutionImpl;
import com.netflix.astyanax.cql.CqlFamilyFactory;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.writes.CqlColumnListMutationImpl.ColumnFamilyMutationContext;
import com.netflix.astyanax.query.ColumnCountQuery;

public class CqlColumnCountQueryImpl implements ColumnCountQuery {

	private final KeyspaceContext ksContext;
	private final ColumnFamilyMutationContext<?, ?> cfContext;
	private final Query query;
	
	public CqlColumnCountQueryImpl(KeyspaceContext ksCtx, ColumnFamilyMutationContext<?,?> cfCtx, Query query) {
		this.ksContext = ksCtx;
		this.cfContext = cfCtx;
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

		public InternalColumnCountExecutionImpl(Query query) {
			super(ksContext, cfContext);
		}

		@Override
		public CassandraOperationType getOperationType() {
			return CassandraOperationType.GET_COLUMN_COUNT;
		}

		@Override
		public Query getQuery() {
			return query;
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
