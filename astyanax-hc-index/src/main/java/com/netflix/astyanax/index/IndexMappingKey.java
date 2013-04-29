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

public class IndexMappingKey<C> {

	private String columnFamily;
	
	private C columnName;

	public IndexMappingKey(String columnFamily,C column) {
		this.columnFamily = columnFamily;
		this.columnName = column;
	}
	
	
	public String getColumnFamily() {
		return columnFamily;
	}


	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}


	public C getColumnName() {
		return columnName;
	}


	public void setColumnName(C columnName) {
		this.columnName = columnName;
	}


	@Override
	public int hashCode() {
		
		//TODO better hashcode
		return columnFamily.hashCode() + columnName.hashCode();
		//return super.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		//could produce class cast
		@SuppressWarnings("unchecked")
		IndexMappingKey<C> cObj = (IndexMappingKey<C>)obj;
		
		if (cObj.columnFamily != null && cObj.columnFamily.equals(columnFamily)
				&& cObj.columnName != null && cObj.columnName.equals(columnName))
			return true;
		
		return false;
	}
	
	
	
}
