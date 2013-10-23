package com.netflix.astyanax.cql.reads;

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlAbstractExecutionImpl;
import com.netflix.astyanax.cql.CqlKeyspaceImpl.KeyspaceContext;
import com.netflix.astyanax.cql.util.CqlTypeMapping;
import com.netflix.astyanax.cql.writes.CqlColumnListMutationImpl.ColumnFamilyMutationContext;
import com.netflix.astyanax.query.RowSliceColumnCountQuery;

@SuppressWarnings("unchecked")
public class CqlRowSliceColumnCountQueryImpl<K> implements RowSliceColumnCountQuery<K> {

	private final KeyspaceContext ksContext;
	private final ColumnFamilyMutationContext<?,?> cfContext;
	private final Query query;
	
	public CqlRowSliceColumnCountQueryImpl(KeyspaceContext ksCtx, ColumnFamilyMutationContext<?,?> cfCtx, Query query) {
		this.ksContext = ksCtx;
		this.cfContext = cfCtx;
		this.query = query;
		
	}

	@Override
	public OperationResult<Map<K, Integer>> execute() throws ConnectionException {
		return new InternalQueryExecutionImpl().execute();
	}

	@Override
	public ListenableFuture<OperationResult<Map<K, Integer>>> executeAsync() throws ConnectionException {
		return new InternalQueryExecutionImpl().executeAsync();
	}
	
	private class InternalQueryExecutionImpl extends CqlAbstractExecutionImpl<Map<K, Integer>> {

		public InternalQueryExecutionImpl() {
			super(ksContext, cfContext);
		}

		@Override
		public CassandraOperationType getOperationType() {
			return CassandraOperationType.GET_ROWS_SLICE;
		}

		@Override
		public Query getQuery() {
			return query;
		}

		@Override
		public Map<K, Integer> parseResultSet(ResultSet resultSet) {
			
			Map<K, Integer> columnCountPerRow = new HashMap<K, Integer>();
			
			for (Row row : resultSet.all()) {
				K key = (K) CqlTypeMapping.getDynamicColumn(row, cf.getKeySerializer(), 0, cf);
				Integer colCount = columnCountPerRow.get(key);
				if (colCount == null) {
					colCount = new Integer(0);
				}	
				colCount = colCount.intValue() + 1;
				columnCountPerRow.put(key, colCount);
			}
			
			return columnCountPerRow;
		}
	}
}
