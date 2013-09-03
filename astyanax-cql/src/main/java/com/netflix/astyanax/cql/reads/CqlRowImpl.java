package com.netflix.astyanax.cql.reads;

import java.nio.ByteBuffer;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;

public class CqlRowImpl<K, C> implements Row<K, C> {

	private com.datastax.driver.core.Row resultRow;
	private CqlColumnListImpl<C> cqlColumnList; 
	
	public CqlRowImpl(com.datastax.driver.core.Row resultRow) {
		this.resultRow = resultRow;
		this.cqlColumnList = new CqlColumnListImpl<C>(resultRow);
	}
	
	@Override
	public K getKey() {
		return (K) resultRow.getString(0);
	}

	@Override
	public ByteBuffer getRawKey() {
		throw new NotImplementedException();
	}

	@Override
	public ColumnList<C> getColumns() {
		return cqlColumnList;
	}
}
