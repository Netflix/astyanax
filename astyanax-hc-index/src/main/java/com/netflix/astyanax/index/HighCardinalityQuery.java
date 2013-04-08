package com.netflix.astyanax.index;

import com.netflix.astyanax.query.RowSliceQuery;

/**
 * This will allow an internal "mapping" of the key of any query that uses {@link Index}
 * 
 *  Without it, the danger is that old values of the index will not be removed
 *  {@link Index#updateIndex(Object, Object, Object, Object)}
 *   and index will resort to 
 *   {@link Index#insertIndex(Object, Object, Object)}
 *   
 *   
 *  
 * 
 * 
 * @author marcus
 *
 * @param <K> - the key type
 * @param <C> - the column type
 * @param <V> - the column value type
 */
public interface HighCardinalityQuery<K, C, V>  {

	
	/**
	 * A wrapped version of the row slice query.
	 * 
	 * @param name
	 * @param value
	 * @return
	 */
	RowSliceQuery<K, C> equals(C name, V value);
	
	//IndexRead<K,C,V> readIndex();
	/**
	 * Register a repair listener that will be informed of changes
	 * when reading from index CF that doesn't make map back to
	 * the original CF that contains the indexed values - which are considered
	 * master values 
	 * 
	 * @param repairListener
	 */
	void registerRepairListener(RepairListener<K, C, V> repairListener);
}
