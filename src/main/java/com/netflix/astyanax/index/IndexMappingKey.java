package com.netflix.astyanax.index;

public class IndexMappingKey<C> {

	private String columnFamily;
	
	private C columnName;

	public IndexMappingKey(String columnFamily,C column) {
		this.columnFamily = columnFamily;
		this.columnName = column;
	}
	
	@Override
	public int hashCode() {
		
		//TODO better hashcode
		return columnFamily.hashCode() + columnName.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		//could produce class cast
		IndexMappingKey<C> cObj = (IndexMappingKey<C>)obj;
		
		if (cObj.columnFamily != null && cObj.columnFamily.equals(columnFamily)
				&& cObj.columnName != null && cObj.equals(columnName))
			return true;
		
		return false;
	}
	
	
	
}
