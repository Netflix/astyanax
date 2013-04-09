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
import java.util.Map;
import java.util.Properties;

import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.cql.CqlStatement;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.partitioner.Partitioner;
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
     */
    AstyanaxConfiguration getConfig();

    /**
     * Returns keyspace name
     */
    String getKeyspaceName();

    /**
     * Return the partitioner for this keyspace.  The partitioner is mainly used for reading
     * all rows.
     * 
     * @return
     * @throws ConnectionException
     */
    Partitioner getPartitioner() throws ConnectionException;
    
    /**
     * Describe the partitioner used by the cluster
     * @throws ConnectionException
     */
    String describePartitioner() throws ConnectionException;

    /**
     * Get a list of all tokens and their endpoints.  This call will return this list of ALL nodes
     * in the cluster, including other regions.  If you are only interested in the subset of
     * nodes for a specific region then use describeRing(dc);
     * @throws ConnectionException
     */
    List<TokenRange> describeRing() throws ConnectionException;

    /**
     * Get a list of all tokens and their endpoints for a specific dc only.
     * 
     * @param dc - null for all dcs
     * @throws ConnectionException
     */
    List<TokenRange> describeRing(String dc) throws ConnectionException;
    
    /**
     * Get a list of tokens and their endpoints for a specific dc/rack combination.
     * @param dc
     * @throws ConnectionException
     */
    List<TokenRange> describeRing(String dc, String rack) throws ConnectionException;

    /**
     * Describe the ring but use the last locally cached version if available.
     * @param cached
     * @throws ConnectionException
     */
    List<TokenRange> describeRing(boolean cached) throws ConnectionException;

    /**
     * Return a complete description of the keyspace and its column families
     * 
     * @throws ConnectionException
     */
    KeyspaceDefinition describeKeyspace() throws ConnectionException;

    /**
     * Get this keyspace's configuration (including column families) as flattened
     * properties
     * @throws ConnectionException 
     */
    Properties getKeyspaceProperties() throws ConnectionException;
    
    /**
     * Get the properties for a column family
     * 
     * @param columnFamily
     * @return
     * @throws ConnectionException 
     */
    Properties getColumnFamilyProperties(String columnFamily) throws ConnectionException;
    
    /**
     * Return the serializer package for a specific column family. This requires
     * a call to the Cassandra cluster and is therefore cached to reduce load on
     * Cassandra and since this data rarely changes.
     * 
     * @param columnFamily
     * @param ignoreErrors
     * @throws ConnectionException
     */
    SerializerPackage getSerializerPackage(String cfName, boolean ignoreErrors) throws ConnectionException,
            UnknownComparatorException;

    /**
     * Prepare a batch mutation object. It is possible to create multiple batch
     * mutations and later merge them into a single mutation by calling
     * mergeShallow on a batch mutation object.
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
     */
    <K, C> ColumnFamilyQuery<K, C> prepareQuery(ColumnFamily<K, C> cf);

    /**
     * Mutation for a single column
     * 
     * @param <K>
     * @param <C>
     * @param columnFamily
     */
    <K, C> ColumnMutation prepareColumnMutation(ColumnFamily<K, C> columnFamily, K rowKey, C column);

    /**
     * Delete all rows in a column family
     * 
     * @param <K>
     * @param <C>
     * @param columnFamily
     * @throws ConnectionException
     * @throws OperationException
     */
    <K, C> OperationResult<Void> truncateColumnFamily(ColumnFamily<K, C> columnFamily) throws OperationException,
            ConnectionException;
    
    /**
     * Delete all rows in a column family
     * 
     * @param columnFamily
     * @throws ConnectionException
     * @throws OperationException
     */
    OperationResult<Void> truncateColumnFamily(String columnFamily) throws ConnectionException;

    /**
     * This method is used for testing purposes only. It is used to inject
     * errors in the connection pool.
     * 
     * @param operation
     * @throws ConnectionException
     */
    OperationResult<Void> testOperation(Operation<?, ?> operation) throws ConnectionException;

    /**
     * This method is used for testing purposes only. It is used to inject
     * errors in the connection pool.
     * 
     * @param operation
     * @throws ConnectionException
     */
    OperationResult<Void> testOperation(Operation<?, ?> operation, RetryPolicy retry) throws ConnectionException;

    /**
     * Create a column family in this keyspace
     * 
     * @param columnFamily
     * @param options - For list of options see http://www.datastax.com/docs/1.0/configuration/storage_configuration
     */
    <K, C>  OperationResult<SchemaChangeResult> createColumnFamily(ColumnFamily<K, C> columnFamily, Map<String, Object> options) throws ConnectionException ;
    
    /**
     * Create a column family in this keyspace using the provided properties.  
     * @param props
     * @return
     * @throws ConnectionException 
     */
    OperationResult<SchemaChangeResult> createColumnFamily(Properties props) throws ConnectionException;
    
    /**
     * Create a column family from the provided options
     * @param options - For list of options see http://www.datastax.com/docs/1.0/configuration/storage_configuration
     * @throws ConnectionException
     */
    OperationResult<SchemaChangeResult> createColumnFamily(Map<String, Object> options) throws ConnectionException ;
    
    /**
     * Update the column family in cassandra
     * 
     * @param columnFamily
     * @param options - For list of options see http://www.datastax.com/docs/1.0/configuration/storage_configuration
     */
    <K, C>  OperationResult<SchemaChangeResult> updateColumnFamily(ColumnFamily<K, C> columnFamily, Map<String, Object> options) throws ConnectionException ;
    
    /**
     * Update the column family definition from properties
     * @param props
     * @throws ConnectionException
     */
    OperationResult<SchemaChangeResult> updateColumnFamily(Properties props) throws ConnectionException ;
    
    /**
     * Update the column family definition from a map of string to object
     * @param props
     * @throws ConnectionException
     */
    OperationResult<SchemaChangeResult> updateColumnFamily(Map<String, Object> options) throws ConnectionException;

    /**
     * Drop a column family from this keyspace
     * @param columnFamilyName
     */
    OperationResult<SchemaChangeResult> dropColumnFamily(String columnFamilyName) throws ConnectionException ;
    
    /**
     * Drop a column family from this keyspace 
     * @param columnFamily
     */
    <K, C>  OperationResult<SchemaChangeResult> dropColumnFamily(ColumnFamily<K, C> columnFamily) throws ConnectionException ;
    
    /**
     * Create the keyspace in cassandra.  This call will only create the keyspace and not 
     * any column families.  Once the keyspace has been created then call createColumnFamily
     * for each CF you want to create.
     * @param options - For list of options see http://www.datastax.com/docs/1.0/configuration/storage_configuration
     */
    OperationResult<SchemaChangeResult> createKeyspace(Map<String, Object> options) throws ConnectionException ;
    
    /**
     * Create the keyspace in cassandra.  This call will create the keyspace and any column families.
     * @param properties
     * @return
     * @throws ConnectionException
     */
    OperationResult<SchemaChangeResult> createKeyspace(Properties properties) throws ConnectionException;
    
    /**
     * Bulk create for a keyspace and a bunch of column famlies
     * @param options
     * @param cfs
     * @throws ConnectionException
     */
    OperationResult<SchemaChangeResult> createKeyspace(Map<String, Object> options, Map<ColumnFamily, Map<String, Object>> cfs) throws ConnectionException ;
    
    /**
     * Update the keyspace in cassandra.
     * @param options - For list of options see http://www.datastax.com/docs/1.0/configuration/storage_configuration
     */
    OperationResult<SchemaChangeResult> updateKeyspace(Map<String, Object> options) throws ConnectionException ;
    
    /**
     * Update the keyspace definition using properties.  Only keyspace options and NO column family options
     * may be set here.
     * 
     * @param props
     * @return
     * @throws ConnectionException
     */
    OperationResult<SchemaChangeResult> updateKeyspace(Properties props) throws ConnectionException;
    
    /**
     * Drop this keyspace from cassandra
     */
    OperationResult<SchemaChangeResult> dropKeyspace() throws ConnectionException ;

    /**
     * List all schema versions in the cluster.  Under normal conditions there
     * should only be one schema.  
     * @return
     * @throws ConnectionException
     */
    Map<String, List<String>> describeSchemaVersions() throws ConnectionException;
    
    /**
     * Prepare a CQL Statement on the keyspace
     * @return
     */
    CqlStatement prepareCqlStatement();
    
    /**
     * Exposes the internal connection pool to the client.  
     * @return
     * @throws ConnectionException
     */
    ConnectionPool<?> getConnectionPool() throws ConnectionException;
}
