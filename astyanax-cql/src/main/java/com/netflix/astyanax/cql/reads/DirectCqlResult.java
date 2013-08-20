package com.netflix.astyanax.cql.reads;

import com.datastax.driver.core.ResultSet;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Rows;

public class DirectCqlResult<K, C> implements CqlResult<K, C> {

	private Long number = null;
	private CqlRowListImpl<K, C> rows; 
	
	public DirectCqlResult(boolean countQuery, ResultSet rs) {
		if (!countQuery) {
			this.rows = new CqlRowListImpl<K, C>(rs.all());
		} else {
			this.number = rs.one().getLong(0);
		}
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
