package com.netflix.astyanax.index;

import java.util.List;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

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
	 * A convenience mechanism for getting the index,
	 * which can be used with;
	 * 
	 * @param keyspace
	 * @param mutation
	 * @return
	 */
	public <C,V,K>Index<C, V, K> getIndex(IndexMetadata<C,K> metaData,Keyspace keyspace,MutationBatch mutation);
	
	
	/**
	 * This may be stored in future, but for now client are expected to add meta data
	 * {@link IndexMetadata} for each life-cycle of the context.
	 * See {@link IndexCoordinationFactory} is currently a singleton
	 * 
	 * 
	 * @param metaData
	 */
	<C,K> void addIndexMetaData(IndexMetadata<C,K> metaData);
	
	/**
	 * Same effect as above but with the additional behaviour that 
	 * the index column family defined in the {@link IndexMetadata}
	 * is created as well.
	 * 
	 * @param metaData
	 */
	<C,K> void addIndexMetaDataAndSchema(Keyspace keyspace,IndexMetadata<C,K> metaData) throws ConnectionException;
	
	<C,K> IndexMetadata<C, K> getMetaData(IndexMappingKey<C> key);
	
	/**
	 * A check to see if we have a local copy of the index meta data {@link IndexMetadata}
	 * via the key
	 * @param key
	 * @return
	 */
	<C> boolean indexExists(IndexMappingKey<C> key);
	
	/**
	 * Same as above method - exposing the index mapping key as composite of it's primitives.
	 * @param cf
	 * @param columnName
	 * @return
	 */
	<C> boolean indexExists(String cf, C columnName);
	
	/**
	 * A column family provided can have multiple {@link IndexMetadata}
	 * hence returns a list.
	 * 
	 * @param cf
	 * @return
	 */
	<C,K> List<IndexMetadata<C, K>> getMetaDataByCf(String cf);
	
	/**
	 * Called internally so that the coordinator can keep track of 
	 * what is being read currently by the index mapping in question.
	 * 
	 * @param mapping
	 * @throws NoMetaDataException
	 */
	<C,V> void reading(IndexMapping<C,V> mapping) throws NoMetaDataException;
	
	/**
	 * Same as above - consider removing.
	 * @param cf
	 * @param columnName
	 * @param value
	 * @throws NoMetaDataException
	 */
	<C,V> void reading(String cf,C columnName,V value) throws NoMetaDataException;
	
	/**
	 * Gets an index mapping by it's key - used internally
	 * @param key
	 * @return
	 */
	<C,V> IndexMapping<C,V> get(IndexMappingKey<C> key);
		
	
	/**
	 * Indicates that a value is mapped by the key is being modified.
	 * Used internally.
	 * 
	 * @param key
	 * @param newValue
	 * @return
	 * @throws NoReadException
	 */
	<C,V> IndexMapping<C,V> modifying(IndexMappingKey<C> key, V newValue) throws NoReadException;
	
	/**
	 * Same as above - used internally.
	 * @param cf
	 * @param columnName
	 * @param newValue
	 * @return
	 * @throws NoReadException
	 */
	<C,V> IndexMapping<C,V> modifying(String cf, C columnName, V newValue) throws NoReadException;
	
	
	
	//These exceptions are currently under consideration
	//but the premise is that they will be thrown when reading a 
	//column family that has no meta data associated with it.
	
	public class NoReadException extends RuntimeException {

		/**
		 * 
		 */
		private static final long serialVersionUID = 3364833516740899944L;
		
	}
	public class NoMetaDataException extends RuntimeException {

		/**
		 * 
		 */
		private static final long serialVersionUID = 126548100391024530L;
		
	}
}
