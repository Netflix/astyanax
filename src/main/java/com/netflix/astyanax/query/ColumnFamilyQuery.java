package com.netflix.astyanax.query;

import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * Top level column family query lets you choose the type of query being performed
 * at the key level.  Single key, key range or a key slice.
 * @author elandau
 *
 * @param <K>
 * @param <C>
 */
public interface ColumnFamilyQuery<K, C> {
	ColumnFamilyQuery<K, C> setConsistencyLevel(ConsistencyLevel consistencyLevel);
	
	/**
	 * Query a single key
	 * @param rowKey
	 * @return
	 */
	RowQuery<K,C> getKey(K rowKey);

	/**
	 * Query a range of keys.  startKey and endKey cannot not be used with the
	 * RandomPartitioner.
	 * 
	 * @param startKey
	 * @param endKey
	 * @param startToken
	 * @param endToken
	 * @param count			Max number of keys to return
	 * @return
	 */
	RowSliceQuery<K,C> getKeyRange(K startKey, K endKey, String startToken, String endToken, int count);
	
	/**
	 * Query a non-contiguous set of keys.
	 * @param keys
	 * @return
	 */
	RowSliceQuery<K,C> getKeySlice(K... keys);
	
	/**
	 * Search for keys matching the provided index clause
	 * @param indexClause
	 * @return
	 */
	IndexQuery<K,C> searchWithIndex();
}
