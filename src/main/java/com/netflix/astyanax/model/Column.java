package com.netflix.astyanax.model;

import com.netflix.astyanax.Serializer;

/**
 * Common interface for extracting column values after a query.  
 * 
 * @author elandau
 *	
 * TODO  Add getIntValue, getDoubleValue, ...
 * 
 * @param <C>  Column name type
 */
public interface Column<C> {

	/**
	 * Column or super column name
	 * @return
	 */
	C getName();

	/**
	 * Return the value 
	 * 
	 * @param <V> value type
	 * @return
	 * @throws NetflixCassandraException 
	 */
	<V> V getValue(Serializer<V> valSer) ;
	
	/**
	 * Return value as a string
	 * 
	 * @return
	 */
	String getStringValue() ;
	
	/**
	 * Return value as an integer
	 * @return
	 */
	int getIntegerValue();
	
	/**
	 * Return value of a counter column
	 * @return
	 */
	long getLongValue();
	
	/**
	 * Get columns in the case of a super column.  
	 * Will throw an exception if this is a regular column 
	 * Valid only if isCompositeColumn returns true
	 * 
	 * @param <C2> Type of column names for sub columns
	 * @return
	 */
	<C2> ColumnList<C2> getSubColumns(Serializer<C2> ser);
	
    /**
     * Returns true if the column contains a list of child columns, otherwise
     * the column contains a value.
     * @return
     */
    boolean isParentColumn();
}
