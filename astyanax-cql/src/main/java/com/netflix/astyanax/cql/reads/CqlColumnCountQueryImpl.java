package com.netflix.astyanax.cql.reads;

import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlAbstractExecutionImpl;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.util.CFQueryContext;
import com.netflix.astyanax.query.ColumnCountQuery;
import com.netflix.astyanax.query.RowQuery;

/**
 * Impl for {@link ColumnCountQuery}
 * 
 * Note that since this query essentially derives itself from the {@link RowQuery} interface, it also uses the statement
 * constructed by the {@link CqlRowQueryImpl} class. The difference in functionality is in how the records form the result set 
 * are parsed. Here we look at the number of rows returned for the same row key. 
 * 
 * Note that since CQL3 can treat columns as rows
 * (depending on the schema), we look for multiple rows with the same row keys. If there are multiple rows, then we count the number 
 * of rows for each unique row key. If there is just one row and the schema definition is like a flat table, then we just count the actual no of data columns returned 
 * in the result set.
 * 
 * See {@link CqlRowQueryImpl} for more details on how the query is actually constructed
 * 
 * @author poberai
 *
 */
public class CqlColumnCountQueryImpl implements ColumnCountQuery {

	private final KeyspaceContext ksContext;
	private final CFQueryContext<?, ?> cfContext;
	private final Statement query;
	
	public CqlColumnCountQueryImpl(KeyspaceContext ksCtx, CFQueryContext<?,?> cfCtx, Statement query) {
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

		public InternalColumnCountExecutionImpl(Statement query) {
			super(ksContext, cfContext);
		}

		@Override
		public CassandraOperationType getOperationType() {
			return CassandraOperationType.GET_COLUMN_COUNT;
		}

		@Override
		public Statement getQuery() {
			return query;
		}

		@Override
		public Integer parseResultSet(ResultSet resultSet) {
			List<Row> rows = resultSet.all();
			if (rows != null) {
				return rows.size();
			} else {
				return 0;
			}
		}
	}
}
