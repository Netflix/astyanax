package com.netflix.astyanax.cql.reads;

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlOperationResultImpl;
import com.netflix.astyanax.cql.util.AsyncOperationResult;
import com.netflix.astyanax.query.RowSliceColumnCountQuery;

@SuppressWarnings("unchecked")
public class CqlRowSliceColumnCountQueryImpl<K> implements RowSliceColumnCountQuery<K> {

	private Cluster cluster; 
	private Query query;
	
	public CqlRowSliceColumnCountQueryImpl(Cluster cluster, Query query) {
		this.cluster = cluster;
		this.query = query;
		
	}

	@Override
	public OperationResult<Map<K, Integer>> execute() throws ConnectionException {
		return parseResponse(cluster.connect().execute(query));
		
	}

	@Override
	public ListenableFuture<OperationResult<Map<K, Integer>>> executeAsync() throws ConnectionException {

		ResultSetFuture rsFuture = cluster.connect().executeAsync(query);
		
		return new AsyncOperationResult<Map<K, Integer>>(rsFuture) {
			@Override
			public OperationResult<Map<K, Integer>> getOperationResult(ResultSet rs) {
				return parseResponse(rs);
			}
		};
	}
	
	private OperationResult<Map<K, Integer>> parseResponse(ResultSet rs) {
		Map<K, Integer> columnCountPerRow = new HashMap<K, Integer>();
		for (Row row : rs.all()) {
			
			K key = (K) row.getString(0); // the first column is the row key
			columnCountPerRow.put(key, row.getColumnDefinitions().size()-1);
		}
		
		return new CqlOperationResultImpl<Map<K, Integer>>(rs, columnCountPerRow);
	}
}
