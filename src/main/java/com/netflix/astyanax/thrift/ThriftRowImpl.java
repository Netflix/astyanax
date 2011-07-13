package com.netflix.astyanax.thrift;

import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;

public class ThriftRowImpl<K,C> implements Row<K,C> {
	private final ColumnList<C> columns;
	private final K key;
	
	public ThriftRowImpl(K key, ColumnList<C> columns) {
		this.key = key;
		this.columns = columns;
	}
	
	@Override
	public K getKey() {
		return key;
	}

	@Override
	public ColumnList<C> getColumns() {
		return columns;
	}

}
