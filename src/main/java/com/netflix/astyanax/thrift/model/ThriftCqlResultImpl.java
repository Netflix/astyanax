package com.netflix.astyanax.thrift.model;

import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Rows;

public class ThriftCqlResultImpl<K,C> implements CqlResult<K,C> {
	private final Rows<K,C> rows;
	private final Integer number;
	
	public ThriftCqlResultImpl(Rows<K,C> rows) {
		this.rows = rows;
		this.number = null;
	}
	
	public ThriftCqlResultImpl(Integer count) {
		this.rows = null;
		this.number = count;
	}
	
	@Override
	public Rows<K, C> getRows() {
		return rows;
	}

	@Override
	public int getNumber() {
		return number;
	}

	@Override
	public boolean hasRows() {
		return rows != null;
	}

	@Override
	public boolean hasNumber() {
		return number != null;
	}

}
