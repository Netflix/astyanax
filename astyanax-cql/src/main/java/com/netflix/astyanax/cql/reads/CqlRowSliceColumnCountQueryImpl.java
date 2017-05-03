/*******************************************************************************
 * Copyright 2011 Netflix
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.cql.reads;

import java.util.HashMap;
import java.util.Map;

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
import com.netflix.astyanax.cql.util.CqlTypeMapping;
import com.netflix.astyanax.query.ColumnCountQuery;
import com.netflix.astyanax.query.RowSliceColumnCountQuery;

/**
 * Impl for {@link RowSliceColumnCountQuery} interface. 
 * Just like {@link ColumnCountQuery}, this class only manages the context for the query. 
 * The actual query statement is supplied from the {@link CqlRowSliceQueryImpl} class.
 * 
 * Note that CQL3 treats columns as rows for certain schemas that contain clustering keys. 
 * Hence this class collapses all {@link ResultSet} rows with the same partition key into a single row
 * when counting all unique rows. 
 *  
 * @author poberai
 *
 * @param <K>
 */
public class CqlRowSliceColumnCountQueryImpl<K> implements RowSliceColumnCountQuery<K> {

	private final KeyspaceContext ksContext;
	private final CFQueryContext<?,?> cfContext;
	private final Statement query;
	
	public CqlRowSliceColumnCountQueryImpl(KeyspaceContext ksCtx, CFQueryContext<?,?> cfCtx, Statement query) {
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
		public Statement getQuery() {
			return query;
		}

		@SuppressWarnings("unchecked")
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
