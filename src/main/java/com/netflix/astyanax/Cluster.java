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

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;

/**
 * Interface for cluster operations.  Use the Keyspace interface to perform
 * keyspace query and mutation operations.
 * 
 * @author elandau
 *
 */
public interface Cluster {
	/**
	 * The cluster name is completely arbitrary
	 * @return
	 * @throws ConnectionException
	 */
	String describeClusterName() throws ConnectionException;

	/**
	 * Return version of cassandra running on the cluster
	 * @return
	 * @throws ConnectionException
	 */
	String getVersion() throws ConnectionException;

	/**
	 * Describe the snitch name used on the cluster
	 * @return
	 * @throws ConnectionException
	 */
	String describeSnitch() throws ConnectionException;

	/**
	 * Describe the partitioner used by the cluster
	 * @return
	 * @throws ConnectionException
	 */
	String describePartitioner() throws ConnectionException;

	/**
	 * Delete the column family from the keyspace
	 * @param columnFamilyName
	 * @return
	 * @throws OperationException
	 * @throws ConnectionException
	 */
	String dropColumnFamily(String keyspaceName, String columnFamilyName) throws OperationException, ConnectionException;

	/**
	 * Delete a keyspace from the cluster
	 * @param keyspaceName
	 * @return
	 * @throws OperationException
	 * @throws ConnectionException
	 */
	String dropKeyspace(String keyspaceName) throws OperationException, ConnectionException;

	/**
	 * Prepare a column family definition.  Call execute() on the returned object
	 * to create the column family.
	 * @return
	 */
	ColumnFamilyDefinition prepareColumnFamilyDefinition();

	/**
	 * Prepare a keyspace definition.  Call execute() on the returned object
	 * to create the keyspace.
	 * 
	 * Not that column families can be added the keyspace definition here
	 * instead of calling prepareColumnFamilyDefinition separately.
	 * 
	 * @return
	 */
	KeyspaceDefinition prepareKeyspaceDefinition();
}
