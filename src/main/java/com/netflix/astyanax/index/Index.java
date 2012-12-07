package com.netflix.astyanax.index;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;


/**
 * High cardinality index utility.
 *  
 * 
 * @author marcus
 *
 * @param <C> - the name of the column to be indexed
 * @param <V> - the value to be indexed 
 * @param <K> - the primary/row key of your referencing CF (reverse look up key)
 */
public interface Index<C,V,K> extends IndexRead<C,V,K>, IndexWrite<C,V,K> {

	
			
	/*
	 * Administrative / expensive operations 
	 */
	
	void buildIndex(String targetCF,C name, Class<K> keyType) throws ConnectionException;
	
	void deleteIndex(String targetCF,C name) throws ConnectionException;
	
	
	
}
