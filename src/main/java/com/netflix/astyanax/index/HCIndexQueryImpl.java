package com.netflix.astyanax.index;

import java.util.concurrent.Future;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.RowSliceQuery;

public class HCIndexQueryImpl<K, C, V> implements HighCardinalityQuery<K, C, V> {

	protected Keyspace keyspace;
	protected ColumnFamily<K, C> columnFamily;
	
	protected Index index;
	
	public HCIndexQueryImpl(Keyspace keyspace,ColumnFamily<K, C> columnFamily) {
		
	}
	@Override
	public OperationResult<Rows<K, C>> execute() throws ConnectionException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Future<OperationResult<Rows<K, C>>> executeAsync()
			throws ConnectionException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RowSliceQuery<K, C> equals(C name, V value) {
		//OK, this is where it happens
		keyspace.prepareQuery(columnFamily);
		
		return null;
	}

}
