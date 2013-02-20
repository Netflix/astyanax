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

import java.util.Collection;

import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;

/**
 * Top level column family query lets you choose the type of query being
 * performed at the key level. Single key, key range or a key slice.
 * 
 * @author elandau
 * 
 * @param <K>
 * @param <C>
 */
public interface ColumnFamilyQuery<K, C> {
    /**
     * Set the consistency level for this operations.
     * 
     * @param consistencyLevel
     */
    ColumnFamilyQuery<K, C> setConsistencyLevel(ConsistencyLevel consistencyLevel);

    /**
     * Set the retry policy to use instead of the default
     * 
     * @param consistencyLevel
     */
    ColumnFamilyQuery<K, C> withRetryPolicy(RetryPolicy retry);

    /**
     * Run the query on the specified host
     * 
     * @param host
     */
    ColumnFamilyQuery<K, C> pinToHost(Host host);

    /**
     * Query a single key
     * 
     * @param rowKey
     */
    RowQuery<K, C> getKey(K rowKey);
    
    /**
     * Query a single row
     * 
     * @param rowKey
     */
    RowQuery<K, C> getRow(K rowKey);

    /**
     * Query a range of keys. startKey and endKey cannot not be used with the
     * RandomPartitioner.
     * 
     * @param startKey
     * @param endKey
     * @param startToken
     * @param endToken
     * @param count
     *            Max number of keys to return
     */
    RowSliceQuery<K, C> getKeyRange(K startKey, K endKey, String startToken, String endToken, int count);
    
    /**
     * Query a range of rows. startKey and endKey cannot not be used with the
     * RandomPartitioner.
     * 
     * @param startKey
     * @param endKey
     * @param startToken
     * @param endToken
     * @param count
     *            Max number of keys to return
     */
    RowSliceQuery<K, C> getRowRange(K startKey, K endKey, String startToken, String endToken, int count);

    /**
     * Query a non-contiguous set of keys.
     * 
     * @param keys
     */
    RowSliceQuery<K, C> getKeySlice(K... keys);
    
    /**
     * Query a non-contiguous set of rows.
     * 
     * @param keys
     */
    RowSliceQuery<K, C> getRowSlice(K... keys);

    /**
     * Query a non-contiguous set of keys.
     * 
     * @param keys
     */
    RowSliceQuery<K, C> getKeySlice(Collection<K> keys);

    /**
     * Query a non-contiguous set of rows.
     * 
     * @param keys
     */
    RowSliceQuery<K, C> getRowSlice(Collection<K> keys);
    
    /**
     * Query a non-contiguous set of keys.
     * 
     * @param keys
     */
    RowSliceQuery<K, C> getKeySlice(Iterable<K> keys);

    /**
     * Query a non-contiguous set of rows.
     * 
     * @param keys
     */
    RowSliceQuery<K, C> getRowSlice(Iterable<K> keys);
    
    /**
     * Query to get an iterator to all rows in the column family
     */
    AllRowsQuery<K, C> getAllRows();

    /**
     * Execute a CQL statement.  Call this for creating a prepared CQL statements.
     * 
     * withCql("...").prepareStatement().execute()
     * 
     * @param cql
     */
    CqlQuery<K, C> withCql(String cql);
    
    /**
     * Search for keys matching the provided index clause
     * 
     * @param indexClause
     */
    IndexQuery<K, C> searchWithIndex();

}
