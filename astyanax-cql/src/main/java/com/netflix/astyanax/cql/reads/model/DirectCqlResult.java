package com.netflix.astyanax.cql.reads.model;

import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Rows;

/**
 * Impl for {@link CqlResult} that parses the {@link ResultSet} from java driver.
 * 
 * @author poberai
 *
 * @param <K>
 * @param <C>
 */
public class DirectCqlResult<K, C> implements CqlResult<K, C> {

	private Long number = null;
	private CqlRowListImpl<K, C> rows; 
	
	public DirectCqlResult(List<Row> rows, ColumnFamily<K,C> cf) {
		this.rows = new CqlRowListImpl<K, C>(rows, cf);
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
