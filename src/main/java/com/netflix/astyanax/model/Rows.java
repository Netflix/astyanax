package com.netflix.astyanax.model;


/**
 * Interface to a collection or Rows with key type K and column type C.
 * The rows can be either super or standard, but not both.
 * @author elandau
 *
 * @param <K>
 * @param <C>
 */
public interface Rows<K, C> extends Iterable<Row<K,C>> {

	/**
	 * Return the row for a specific key.  Will return an exception if the result
	 * set is a list and not a lookup.
	 * @param key
	 * @return
	 */
	Row<K,C> getRow(K key);

    /** 
     * Get the number of rows in the list
     * 
     * @return integer representing the number of rows in the list
     */
    int size();
    
    /**
     * Determine if the row list has data
     * 
     * @return
     */
    boolean isEmpty();
	
}
