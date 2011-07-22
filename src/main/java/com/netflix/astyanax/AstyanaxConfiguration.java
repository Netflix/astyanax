package com.netflix.astyanax;

import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * Interface defining all connection pool configuration parameters.
 * 
 * @author elandau
 *
 */
public interface AstyanaxConfiguration extends ConnectionPoolConfiguration {
	public interface Factory<T> {
		T createInstance(AstyanaxConfiguration config);
	}
	
	/**
	 * Default consistency level used when reading from the cluster.  This
	 * value can be overwritten on the Query operations (returned by 
	 * Keyspace.prepareXXQuery) by calling Query.setConsistencyLevel().
	 * @return
	 */
	ConsistencyLevel getDefaultReadConsistencyLevel();

	/**
	 * Default consistency level used when reading from the cluster.  This
	 * value can be overwritten on MutationBatch operation (returned by 
	 * Keyspace.prepareMutationBatch) by calling MutationBatch.setConsistencyLevel().
	 * @return
	 */
	ConsistencyLevel getDefaultWriteConsistencyLevel();

	/**
	 * Return clock to use when setting timestamps for column insertion and 
	 * deletion operations.
	 * @return
	 */
	Clock getClock();
	
	/**
	 * Return tracer to receive operation completion notification.
	 * @see com.netflix.astyanax.connectionpool.KeyspaceTracers
	 * @return
	 */
	KeyspaceTracers getKeyspaceTracers();

	/**
	 * If returns true then debug messages will be logged
	 * @return
	 */
	public boolean isDebugEnabled();
}