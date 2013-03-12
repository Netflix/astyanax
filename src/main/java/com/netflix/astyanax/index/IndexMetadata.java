package com.netflix.astyanax.index;

import java.util.Map;

/**
 * 
 * @author marcus
 *
 */
public class IndexMetadata<C,K> {

	private IndexMappingKey<C> indexKey;
	
	private Class<K> rowKeyClass;

	/**
	 * Indicated the column family that will represent this index
	 */
	private String indexCFName = IndexImpl.DEFAULT_INDEX_CF;
	
	private Map<String,Object> indexCFOptions = null;
	
	public IndexMetadata(IndexMappingKey<C> indexKey, Class<K> rowKeyClass) {
		this.indexKey = indexKey;
		this.rowKeyClass = rowKeyClass;
	}
	public IndexMetadata(IndexMappingKey<C> indexKey, Class<K> rowKeyClass,String indexCFName) {
		this.indexKey = indexKey;
		this.rowKeyClass = rowKeyClass;
		this.indexCFName = indexCFName;
	}
	public IndexMetadata(String cf, C columnName, Class<K> rowKeyClass) {
		this (new IndexMappingKey<C>(cf, columnName),rowKeyClass);
	}
	
	public IndexMetadata(String cf, C columnName, Class<K> rowKeyClass,String indexCFName) {
		this (new IndexMappingKey<C>(cf, columnName),rowKeyClass);
		this.indexCFName = indexCFName;
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
	public String getIndexCFName() {
		return indexCFName;
	}
	public void setIndexCFName(String indexCFName) {
		this.indexCFName = indexCFName;
	}
	public Map<String, Object> getIndexCFOptions() {
		return indexCFOptions;
	}
	public void setIndexCFOptions(Map<String, Object> indexCFOptions) {
		this.indexCFOptions = indexCFOptions;
	}
	
	
	
	
}
