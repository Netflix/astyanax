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
