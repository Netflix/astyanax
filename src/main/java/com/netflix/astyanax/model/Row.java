package com.netflix.astyanax.model;


/**
 * Instance of a row with key type K and column name type C.  
 * Child columns can be either standard columns or super columns
 * 
 * @author elandau
 *
 * @param <K>
 * @param <C>
 */
public interface Row<K, C>  {
	/**
	 * Return the key value
	 * 
	 * @return
	 */
	K getKey();
	
	/**
	 * Child columns of the row.  Note that if a ColumnPath was provided to a 
	 * query these will be the columns at the column path location and not the
	 * columns at the root of the row.
	 * 
	 * @return
	 */
	ColumnList<C> getColumns();
}
