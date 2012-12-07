package com.netflix.astyanax.index;

/**
 * 
 * @author marcus
 *
 */
public class IndexMetadata<C,K> {

	private IndexMappingKey<C> indexKey;
	
	private Class<K> rowKeyClass;

	public IndexMetadata(IndexMappingKey<C> indexKey, Class<K> rowKeyClass) {
		this.indexKey = indexKey;
		this.rowKeyClass = rowKeyClass;
	}
	public IndexMetadata(String cf, C columnName, Class<K> rowKeyClass) {
		this (new IndexMappingKey<C>(cf, columnName),rowKeyClass);
	}
	
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
