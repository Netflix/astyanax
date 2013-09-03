package com.netflix.astyanax.cql.reads;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlAbstractExecutionImpl;
import com.netflix.astyanax.cql.CqlFamilyFactory;
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
		return new InternalColumnQueryExecutionImpl().execute();
	}

	@Override
	public ListenableFuture<OperationResult<Column<C>>> executeAsync() throws ConnectionException {
		return new InternalColumnQueryExecutionImpl().executeAsync();
	}

	private class InternalColumnQueryExecutionImpl extends CqlAbstractExecutionImpl<Column<C>> {

		private final Object rowKey = context.getNext(Object.class); 
		private final Object column = context.getNext(Object.class); 

		public InternalColumnQueryExecutionImpl() {
			super(context);
		}

		@Override
		public CassandraOperationType getOperationType() {
			return CassandraOperationType.GET_COLUMN;
		}

		@Override
		public Query getQuery() {
			
			
			Query query = null; 
			if (CqlFamilyFactory.OldStyleThriftMode()) {
				
				 query = QueryBuilder.select("column1", "value")
						  .from(keyspace, cf.getName())
						  .where(eq("key", rowKey))
						  .and(eq("column1", column));
				
			} else {
				query = QueryBuilder.select(String.valueOf(column))
						.from(keyspace, cf.getName())
						.where(eq(cf.getKeyAlias(), rowKey));
			}
			return query;
		}

		@Override
		public Column<C> parseResultSet(ResultSet rs) {
			
			Row row = rs.one();
			if (row == null) {
				return new CqlColumnImpl();
			}
			
			String columnName = null;
			int columnIndex = -1;
			
			if (CqlFamilyFactory.OldStyleThriftMode()) {
				columnName = row.getString("column1");
				columnIndex = 1;
			} else {
				columnName = String.valueOf(column);
				columnIndex = 0;
			}
			
			return new CqlColumnImpl<C>((C) columnName, row, columnIndex);
		}
	}
}
