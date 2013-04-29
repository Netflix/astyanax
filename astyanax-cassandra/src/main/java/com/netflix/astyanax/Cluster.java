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

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.ddl.SchemaChangeResult;

/**
 * Interface for cluster operations. Use the Keyspace interface to perform
 * keyspace query and mutation operations.
 * 
 * @author elandau
 * 
 */
public interface Cluster {
    /**
     * The cluster name is completely arbitrary
     * @throws ConnectionException
     */
    String describeClusterName() throws ConnectionException;

    /**
     * Return version of cassandra running on the cluster
     * @throws ConnectionException
     */
    String getVersion() throws ConnectionException;

    /**
     * Describe the snitch name used on the cluster
     * @throws ConnectionException
     */
    String describeSnitch() throws ConnectionException;

    /**
     * Describe the partitioner used by the cluster
     * @throws ConnectionException
     */
    String describePartitioner() throws ConnectionException;

    /**
     * For each schema version present in the cluster, returns a list of nodes at that 
     * version. Hosts that do not respond will be under the key DatabaseDescriptor.INITIAL_VERSION. 
     * The cluster is all on the same version if the size of the map is 1
     * @throws ConnectionException
     */
    Map<String, List<String>> describeSchemaVersions() throws ConnectionException;

    /**
     * Prepare a column family definition. Call execute() on the returned object
     * to create the column family.
     */
    ColumnFamilyDefinition makeColumnFamilyDefinition();

    /**
     * Make a column definitio to be added to a ColumnFamilyDefinition
     */
    ColumnDefinition makeColumnDefinition();

    /**
     * Delete the column family from the keyspace
     * 
     * @param  columnFamilyName To delete
     * @throws OperationException
     * @throws ConnectionException
     */
    OperationResult<SchemaChangeResult> dropColumnFamily(String keyspaceName, String columnFamilyName) throws ConnectionException;

    /**
     * Add a column family to an existing keyspace
     * 
     * @param def - Created by calling makeColumnFamilyDefinition();
     * @throws ConnectionException
     */
    OperationResult<SchemaChangeResult> addColumnFamily(ColumnFamilyDefinition def) throws ConnectionException;

    /**
     * Update an existing column family
     * 
     * @param def - Created by calling makeColumnFamilyDefinition();
     * @throws ConnectionException
     */
    OperationResult<SchemaChangeResult> updateColumnFamily(ColumnFamilyDefinition def) throws ConnectionException;

    /**
     * Prepare a keyspace definition. Call execute() on the returned object to
     * create the keyspace.
     * 
     * Not that column families can be added the keyspace definition here
     * instead of calling makeColumnFamilyDefinition separately.
     */
    KeyspaceDefinition makeKeyspaceDefinition();

    /**
     * Return all the keyspace definition as properties
     * @param keyspace
     * @return
     * @throws ConnectionException 
     */
    Properties getAllKeyspaceProperties() throws ConnectionException;
    
    /**
     * Return the keyspace definition as a properties
     * @return
     */
    Properties getKeyspaceProperties(String keyspace) throws ConnectionException ;
    
    /**
     * Return the 
     * @return
     */
    Properties getColumnFamilyProperties(String keyspace, String keyspaces) throws ConnectionException ;
    
    /**
     * Return details about all keyspaces in the cluster
     * @throws ConnectionException
     */
    List<KeyspaceDefinition> describeKeyspaces() throws ConnectionException;

    /**
     * Describe a single keyspace
     * 
     * @param ksName - Keyspace name
     * @throws ConnectionException
     */
    KeyspaceDefinition describeKeyspace(String ksName) throws ConnectionException;

    /**
     * Return a keyspace client. Note that this keyspace will use the same
     * connection pool as the cluster and any other keyspaces created from this
     * cluster instance. As a result each keyspace operation is likely to have
     * some overhead for switching keyspaces.
     */
    Keyspace getKeyspace(String keyspace) throws ConnectionException ;

    /**
     * Delete a keyspace from the cluster
     * 
     * @param keyspaceName - Keyspace to drop
     * @throws OperationException
     * @throws ConnectionException
     */
    OperationResult<SchemaChangeResult> dropKeyspace(String keyspaceName) throws ConnectionException;

    /**
     * Add a new keyspace to the cluster. The keyspace object may include column
     * families as well. Create a KeyspaceDefinition object by calling
     * makeKeyspaceDefinition().
     * 
     * @param def
     * @return
     */
    OperationResult<SchemaChangeResult> addKeyspace(KeyspaceDefinition def) throws ConnectionException;

    /**
     * Update a new keyspace in the cluster. The keyspace object may include
     * column families as well. Create a KeyspaceDefinition object by calling
     * makeKeyspaceDefinition().
     * 
     * @param def
     */
    OperationResult<SchemaChangeResult> updateKeyspace(KeyspaceDefinition def) throws ConnectionException;

    /**
     * Configuration object for this Cluster
     */
    AstyanaxConfiguration getConfig();
    
    /**
     * Create a column family.  Definition must also specify the parent keyspace.
     * @param options - For list of options see http://www.datastax.com/docs/1.0/configuration/storage_configuration
     */
    OperationResult<SchemaChangeResult> createColumnFamily(Map<String, Object> options) throws ConnectionException ;
    
    /**
     * Create a column family.  Definition must also specify the parent keyspace.
     * @param options - For list of options see http://www.datastax.com/docs/1.0/configuration/storage_configuration
     */
    OperationResult<SchemaChangeResult> createColumnFamily(Properties props) throws ConnectionException ;
    
    /**
     * Update the column family in cassandra
     * 
     * @param options - For list of options see http://www.datastax.com/docs/1.0/configuration/storage_configuration
     */
    OperationResult<SchemaChangeResult> updateColumnFamily(Map<String, Object> options) throws ConnectionException ;
    
    /**
     * Update the column family in cassandra
     * 
     * @param options - For list of options see http://www.datastax.com/docs/1.0/configuration/storage_configuration
     */
    OperationResult<SchemaChangeResult> updateColumnFamily(Properties props) throws ConnectionException ;
    
    /**
     * Create the keyspace in cassandra.  This call will only create the keyspace and not 
     * any column families.  Once the keyspace has been created then call createColumnFamily
     * for each CF you want to create.
     * @param options - For list of options see http://www.datastax.com/docs/1.0/configuration/storage_configuration
     */
    OperationResult<SchemaChangeResult> createKeyspace(Map<String, Object> options) throws ConnectionException ;
    
    /**
     * Create the keyspace in cassandra.  This call will only create the keyspace and any 
     * child column families.  
     * 
     * @param options - For list of options see http://www.datastax.com/docs/1.0/configuration/storage_configuration
     */
    OperationResult<SchemaChangeResult> createKeyspace(Properties props) throws ConnectionException;
    
    /**
     * Update the keyspace in cassandra.  Only the keyspace and no column family definitions will be 
     * updated here
     * @param options - For list of options see http://www.datastax.com/docs/1.0/configuration/storage_configuration
     */
    OperationResult<SchemaChangeResult> updateKeyspace(Map<String, Object> options) throws ConnectionException ;
    
    /**
     * Update the keyspace in cassandra.  Only the keyspace and no column family definitions will be 
     * updated here
     * @param options - For list of options see http://www.datastax.com/docs/1.0/configuration/storage_configuration
     */
    OperationResult<SchemaChangeResult> updateKeyspace(Properties props) throws ConnectionException ;
    
}
