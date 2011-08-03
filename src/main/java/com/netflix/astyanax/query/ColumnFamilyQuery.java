/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
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
	 * Query to get an iterator to all rows in the column family
	 * @return
	 */
	AllRowsQuery<K,C> getAllRows();
	
	/**
	 * Prepare a CQL Query
	 * @param cql
	 * @return
	 */
	CqlQuery<K, C> withCql(String cql);
	
	/**
	 * Search for keys matching the provided index clause
	 * @param indexClause
	 * @return
	 */
	IndexQuery<K,C> searchWithIndex();
}
