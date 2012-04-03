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
package com.netflix.astyanax;

import java.util.List;

import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.serializers.UnknownComparatorException;

/**
 * Interface providing access to mutate and query columns from a cassandra
 * keyspace.
 * 
 * @author elandau
 * 
 */
public interface Keyspace {
    /**
     * Return the configuration object used to set up this keyspace
     * 
     * @return
     */
    AstyanaxConfiguration getConfig();

    /**
     * Returns keyspace name
     * 
     * @return
     */
    String getKeyspaceName();

    /**
     * Get a list of all tokens and their endpoints
     * 
     * @return
     * @throws ConnectionException
     */
    List<TokenRange> describeRing() throws ConnectionException;

    /**
     * Return a complete description of the keyspace and its column families
     * 
     * @return
     * @throws ConnectionException
     */
    KeyspaceDefinition describeKeyspace() throws ConnectionException;

    /**
     * Return the serializer package for a specific column family. This requires
     * a call to the Cassandra cluster and is therefore cached to reduce load on
     * Cassandra and since this data rarely changes.
     * 
     * @param columnFamily
     * @param ignoreErrors
     * @return
     * @throws ConnectionException
     */
    SerializerPackage getSerializerPackage(String cfName, boolean ignoreErrors)
            throws ConnectionException, UnknownComparatorException;

    /**
     * Prepare a batch mutation object. It is possible to create multiple batch
     * mutations and later merge them into a single mutation by calling
     * mergeShallow on a batch mutation object.
     * 
     * @return
     * @throws ConnectionException
     */
    MutationBatch prepareMutationBatch();

    /**
     * Starting point for constructing a query. From the column family the
     * client can perform all 4 types of queries: get column, get key slice, get
     * key range and and index query.
     * 
     * @param <K>
     * @param <C>
     * @param cf
     *            Column family to be used for the query. The key and column
     *            serializers in the ColumnFamily are automatically used while
     *            constructing the query and the response.
     * @return
     */
    <K, C> ColumnFamilyQuery<K, C> prepareQuery(ColumnFamily<K, C> cf);

    /**
     * Mutation for a single column
     * 
     * @param <K>
     * @param <C>
     * @param columnFamily
     * @return
     */
    <K, C> ColumnMutation prepareColumnMutation(
            ColumnFamily<K, C> columnFamily, K rowKey, C column);

    /**
     * Delete all rows in a column family
     * 
     * @param <K>
     * @param <C>
     * @param columnFamily
     * @return
     * @throws ConnectionException
     * @throws OperationException
     */
    <K, C> OperationResult<Void> truncateColumnFamily(
            ColumnFamily<K, C> columnFamily) throws OperationException,
            ConnectionException;

    /**
     * This method is used for testing purposes only. It is used to inject
     * errors in the connection pool.
     * 
     * @param operation
     * @return
     * @throws ConnectionException
     */
    OperationResult<Void> testOperation(Operation<?, ?> operation)
            throws ConnectionException;

    /**
     * This method is used for testing purposes only. It is used to inject
     * errors in the connection pool.
     * 
     * @param operation
     * @return
     * @throws ConnectionException
     */
    OperationResult<Void> testOperation(Operation<?, ?> operation,
            RetryPolicy retry) throws ConnectionException;

}
