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
import com.netflix.astyanax.consistency.ConsistencyLevelPolicy;
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
     * Set the consistency level policy for this operations.
     * 
     * @param consistencyLevelPolicy
     * @return
     */
    ColumnFamilyQuery<K, C> setConsistencyLevelPolicy(ConsistencyLevelPolicy consistencyLevelPolicy);

    /**
     * Set the retry policy to use instead of the default
     * 
     * @param retry
     * @return
     */
    ColumnFamilyQuery<K, C> withRetryPolicy(RetryPolicy retry);

    /**
     * Run the query on the specified host
     * 
     * @param host
     * @return
     */
    ColumnFamilyQuery<K, C> pinToHost(Host host);

    /**
     * Query a single key
     * 
     * @param rowKey
     * @return
     */
    RowQuery<K, C> getKey(K rowKey);

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
     * @return
     */
    RowSliceQuery<K, C> getKeyRange(K startKey, K endKey, String startToken, String endToken, int count);

    /**
     * Query a non-contiguous set of keys.
     * 
     * @param keys
     * @return
     */
    RowSliceQuery<K, C> getKeySlice(K... keys);

    /**
     * Query a non-contiguous set of keys.
     * 
     * @param keys
     * @return
     */
    RowSliceQuery<K, C> getKeySlice(Collection<K> keys);

    /**
     * Query a non-contiguous set of keys.
     * 
     * @param keys
     * @return
     */
    RowSliceQuery<K, C> getKeySlice(Iterable<K> keys);

    /**
     * Query to get an iterator to all rows in the column family
     * 
     * @return
     */
    AllRowsQuery<K, C> getAllRows();

    /**
     * Prepare a CQL Query
     * 
     * @param cql
     * @return
     */
    CqlQuery<K, C> withCql(String cql);

    /**
     * Search for keys matching the provided index clause
     * 
     * @return
     */
    IndexQuery<K, C> searchWithIndex();

}
