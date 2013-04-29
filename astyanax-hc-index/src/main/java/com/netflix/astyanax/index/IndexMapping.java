/* 
 * Copyright (c) 2013 Research In Motion Limited. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 * 
 */
package com.netflix.astyanax.index;


/**
 * Every time an indexed column gets "updated" we need to keep that old value.
 * This is the meta data that stores it.
 * 
 * This now gets exposed through the {@link RepairListener} interface.
 * 
 * @author marcus
 *
 * @param <C> - the column type
 * @param <V> - This is the value of the column
 * 
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
	public IndexMapping(IndexMappingKey<C> key,V valueOfCol) {
		this(key,valueOfCol,null);
		
		
	}
	public IndexMapping(String columnFamily,C columnName,V valueOfCol, V oldValue) {
		this(new IndexMappingKey<C>(columnFamily,columnName),valueOfCol,oldValue);
				
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
