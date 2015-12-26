package com.netflix.astyanax.cql.reads.model;

import java.nio.ByteBuffer;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.netflix.astyanax.cql.util.CqlTypeMapping;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;

/**
 * Impl for {@link Row} that parses the {@link ResultSet} from java driver and translates back to Astyanax Row. 
 * Note that if your schema has a clustering key, then each individual row from the result set is a unique column, 
 * and all result set rows with the same partition key map to a unique Astyanax row. 
 * 
 * @author poberai
 *
 * @param <K>
 * @param <C>
 */
@SuppressWarnings("unchecked")
public class CqlRowImpl<K, C> implements Row<K, C> {

	private final K rowKey;
	private final CqlColumnListImpl<C> cqlColumnList;
	private final ColumnFamily<K, C> cf;
	
	public CqlRowImpl(com.datastax.driver.core.Row resultRow, ColumnFamily<K, C> cf) {
		this.rowKey = (K) getRowKey(resultRow, cf);
		this.cqlColumnList = new CqlColumnListImpl<C>(resultRow, cf);
		this.cf = cf;
	}
	
	public CqlRowImpl(List<com.datastax.driver.core.Row> rows, ColumnFamily<K, C> cf) {
		this.rowKey = (K) getRowKey(rows.get(0), cf);
		this.cqlColumnList = new CqlColumnListImpl<C>(rows, cf);
		this.cf = cf;
	}
	
	public CqlRowImpl(K rKey, CqlColumnListImpl<C> colList, ColumnFamily<K, C> columnFamily) {
		this.rowKey = rKey;
		this.cqlColumnList = colList;
		this.cf = columnFamily;
	}
	
	@Override
	public K getKey() {
		return rowKey;
	}

	@Override
	public ByteBuffer getRawKey() {
		return cf.getKeySerializer().toByteBuffer(rowKey);
	}

	@Override
	public ColumnList<C> getColumns() {
		return cqlColumnList;
	}
	
	private Object getRowKey(com.datastax.driver.core.Row row, ColumnFamily<K, C> cf) {
		return CqlTypeMapping.getDynamicColumn(row, cf.getKeySerializer(), 0, cf);
	}
}
