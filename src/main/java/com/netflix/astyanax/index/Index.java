package com.netflix.astyanax.index;

import java.util.Collection;

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
public interface Index<C,V,K> {

	
	
	//void createIndex(String columnFamilyName);
	
	
	void insertIndex(C name,V value, K pkValue) throws ConnectionException;
	
	void updateIndex(C name,V value, V oldValue,  K pkValue) throws ConnectionException;
	
	//
	//The only operation supported currently
	//
	Collection<K> eq(C name,V value) throws ConnectionException;
		
	
	void buildIndex(String targetCF,C name, Class<K> keyType) throws ConnectionException;
	
	void deleteIndex(String targetCF,C name) throws ConnectionException;
	
	
	
}
