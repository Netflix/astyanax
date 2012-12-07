package com.netflix.astyanax.index;

/**
 * 
 * @author marcus
 *
 */
public class IndexMetadata<C,K> {

	private IndexMappingKey<C> indexKey;
	
	private Class<K> rowKeyClass;

	public IndexMappingKey<C> getIndexKey() {
		return indexKey;
	}

	public void setIndexKey(IndexMappingKey<C> indexKey) {
		this.indexKey = indexKey;
	}

	public Class<K> getRowKeyClass() {
		return rowKeyClass;
	}

	public void setRowKeyClass(Class<K> rowKeyClass) {
		this.rowKeyClass = rowKeyClass;
	}
	
	
	
	
}
