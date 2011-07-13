package com.netflix.astyanax.model;

import com.netflix.astyanax.Serializer;

/**
 * Interface to a list of columns.
 * 
 * @author elandau
 *
 * @param <C>	Data type for column names
 */
public interface ColumnList<C> extends Iterable<Column<C>>{
    /** 
     * Queries column by name
     * 
     * @param columnName
     * @return an instance of a column or null if not found
     * @throws Exception 
     */
    Column<C> getColumnByName(C columnName) ;

    /**
     * Queries column by index
     * @param idx
     * @return
     * @throws NetflixCassandraException 
     */
    Column<C> getColumnByIndex(int idx);
    
    /**
     * Return the super column with the specified name
     * @param <C2>
     * @param columnName
     * @param colSer
     * @return
     * @throws NetflixCassandraException 
     */
    <C2> Column<C2> getSuperColumn(C columnName, Serializer<C2> colSer);
    
    /**
     * Get super column by index
     * @param idx
     * @return
     * @throws NetflixCassandraException 
     */
    <C2> Column<C2> getSuperColumn(int idx, Serializer<C2> colSer);
    
    /** 
     * Indicates if the list of columns is empty
     * 
     * @return
     */
    boolean isEmpty();
    
    /** 
     * returns the number of columns in the row
     * 
     * @return
     */
    int size();
	
    /**
     * Returns true if the columns are super columns with subcolumns.  If true
     * then use getSuperColumn to call children.  Otherwise call getColumnByIndex
     * and getColumnByName to get the standard columns in the list.
     * @return
     */
    boolean isSuperColumn();
}
