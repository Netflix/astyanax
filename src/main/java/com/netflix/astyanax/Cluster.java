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
