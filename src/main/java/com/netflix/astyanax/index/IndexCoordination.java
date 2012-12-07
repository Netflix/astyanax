package com.netflix.astyanax.index;

/**
 * 
 * Ties the reading and the writing together locally on the client.
 * 
 * To be used in conjunction with {@link HighCardinalityQuery} for reads
 * and then {@link IndexedMutationBatch} for writes.
 * These names are consistent with Astyanax and Cassandra naming.
 * 
 * 
 * 
 * @author marcus
 *
 */
public interface IndexCoordination {

	/**
	 * This may be stored in future, but for now client are expected to add this
	 * for each life-cycle of the context.
	 * 
	 * @param metaData
	 */
	<C,K> void addIndexMetaData(IndexMetadata<C,K> metaData);
	
	<C,K> IndexMetadata<C, K> getMetaData(IndexMappingKey<C> key);
	
	<C> boolean indexExists(IndexMappingKey<C> key);
	
	<C> boolean indexExists(String cf, C columnName);
	
	
	<C,V> void reading(IndexMapping<C,V> mapping) throws NoMetaDataException;
	
	<C,V> void reading(String cf,C columnName,V value) throws NoMetaDataException;
	
	
	<C,V> IndexMapping<C,V> get(IndexMappingKey<C> key);
		
	
	
	<C,V> void modifying(IndexMappingKey<C> key, V newValue) throws NoReadException;
	
	
	<C,V> void modifying(String cf, C columnName, V newValue) throws NoReadException;
	
	
	//checked or unchecked, that is the question!!
	public class NoReadException extends RuntimeException {
		
	}
	public class NoMetaDataException extends RuntimeException {
		
	}
}
