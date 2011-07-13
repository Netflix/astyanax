package com.netflix.astyanax;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

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
}
