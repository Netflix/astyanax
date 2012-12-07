package com.netflix.astyanax.index;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;

import com.netflix.astyanax.model.ColumnFamily;

/**
 * Wrapping the batch mutation allows for index meta data to be maintained.
 * 
 * This is to be used in concert with {@link HighCardinalityQuery}.
 * 
 * The supported use case:
 * Add indexMetaData using  {@link IndexCoordination#addIndexMetaData(IndexMetadata)}
 * this is a one time operation, and could be persisted in the future.
 * 
 * {@link HighCardinalityQuery#equals(Object)}
 * 
 * @author marcus
 *
 */
public interface IndexedMutationBatch  {

	/**
	 * Allows the column family to be indexed through this muation operation.
	 * 
	 * @param currentBatch - get this from {@link Keyspace#prepareColumnMutation(ColumnFamily, Object, Object)}
	 * @param columnFamily
	 * @param rowKey
	 * @return
	 */
	<K, C> ColumnListMutation<C> withIndexedRow(MutationBatch currentBatch, ColumnFamily<K, C> columnFamily, K rowKey);
	
	
}
