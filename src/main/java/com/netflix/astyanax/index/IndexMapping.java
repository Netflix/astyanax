package com.netflix.astyanax.index;

import com.netflix.astyanax.query.ColumnFamilyQuery;

/**
 * Every time an indexed column gets "updated" we need to keep that old value.
 * This is the meta data that stores it.
 * 
 * @author marcus
 *
 * @param <C>
 * @param <V>
 */
public class IndexMapping<C,V> {

	private IndexMappingKey<C> colKey;
	
	private V valueOfCol;
	
	private V oldValueofCol;

	public IndexMapping() {}
	
	public IndexMapping(IndexMappingKey<C> key) {
		this.colKey = key;
	}
	public IndexMapping(IndexMappingKey<C> key,V valueOfCol, V oldValue) {
		this(key);
		this.valueOfCol = valueOfCol;
		this.oldValueofCol = oldValue;
	}
	public IndexMapping(String columnFamily,C columnName,V valueOfCol, V oldValue) {
		this(new IndexMappingKey<C>(columnFamily,columnName));
		this.valueOfCol = valueOfCol;
		this.oldValueofCol = oldValue;
	}
	
	
	public IndexMappingKey<C> getColKey() {
		return colKey;
	}

	public void setColKey(IndexMappingKey<C> colKey) {
		this.colKey = colKey;
	}

	public V getValueOfCol() {
		return valueOfCol;
	}

	public void setValueOfCol(V valueOfCol) {
		this.valueOfCol = valueOfCol;
	}

	public V getOldValueofCol() {
		return oldValueofCol;
	}

	public void setOldValueofCol(V oldValueofCol) {
		this.oldValueofCol = oldValueofCol;
	}
	
	
	
}
