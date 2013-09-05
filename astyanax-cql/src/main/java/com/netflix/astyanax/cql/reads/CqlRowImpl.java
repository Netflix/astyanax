package com.netflix.astyanax.cql.reads;

import java.nio.ByteBuffer;
import java.util.List;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.netflix.astyanax.cql.util.CqlTypeMapping;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;

public class CqlRowImpl<K, C> implements Row<K, C> {

	private K rowKey;
	private CqlColumnListImpl<C> cqlColumnList; 
	
	public CqlRowImpl(com.datastax.driver.core.Row resultRow, ColumnFamily<?, ?> cf) {
		this.rowKey = (K) getRowKey(resultRow, cf);
		this.cqlColumnList = new CqlColumnListImpl<C>(resultRow);
	}
	
	public CqlRowImpl(List<com.datastax.driver.core.Row> rows, ColumnFamily<?, ?> cf) {
		this.rowKey = (K) getRowKey(rows.get(0), cf);
		this.cqlColumnList = new CqlColumnListImpl<C>(rows, cf);
	}
	
	@Override
	public K getKey() {
		return rowKey;
	}

	@Override
	public ByteBuffer getRawKey() {
		throw new NotImplementedException();
	}

	@Override
	public ColumnList<C> getColumns() {
		return cqlColumnList;
	}
	
	private Object getRowKey(com.datastax.driver.core.Row row, ColumnFamily<?, ?> cf) {
		return CqlTypeMapping.getDynamicColumnName(row, cf.getKeySerializer(), cf.getKeyAlias());
	}
}
