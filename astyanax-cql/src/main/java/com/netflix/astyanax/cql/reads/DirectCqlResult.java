package com.netflix.astyanax.cql.reads;

import java.util.List;

import com.datastax.driver.core.Row;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Rows;

public class DirectCqlResult<K, C> implements CqlResult<K, C> {

	private Long number = null;
	private CqlRowListImpl<K, C> rows; 
	
	public DirectCqlResult(List<Row> rows, ColumnFamily<K,C> cf, boolean oldStyle) {
		this.rows = new CqlRowListImpl<K, C>(rows, cf, oldStyle);
	}

	public DirectCqlResult(Long number) {
		this.number = number;
	}
	
	@Override
	public Rows<K, C> getRows() {
		return this.rows;
	}

	@Override
	public int getNumber() {
		return number.intValue();
	}

	@Override
	public boolean hasRows() {
		return rows != null;
	}

	@Override
	public boolean hasNumber() {
		return this.number != null;
	}
}
