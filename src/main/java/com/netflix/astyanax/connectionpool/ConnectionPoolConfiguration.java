package com.netflix.astyanax.connectionpool;

import java.util.List;

import com.netflix.astyanax.Clock;
import com.netflix.astyanax.KeyspaceTracers;
import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * Interface defining all connection pool configuration parameters.
 * 
 * @author elandau
 *
 */
public interface ConnectionPoolConfiguration {
	public interface Factory<T> {
		T createInstance(ConnectionPoolConfiguration config);
	}
	
	/**
	 * Keyspace name for this connection pool.  A connection pool is associated
	 * with only one keyspace
	 * @return
	 */
	String getKeyspaceName();

	/**
	 * Application name in discovery with which the cluster nodes register.
	 * This name is used to refresh hosts from discovery.
	 * @return
	 */
	String getClusterName();
	
	/**
	 * True if auto discovery should use a ring_describe
	 * @return
	 */
	boolean isRingDescribeEnabled();

	/**
	 * Socket connect/read/write timeout
	 * @return
	 */
	int getSocketTimeout();

	/**
	 * List of comma delimited host:port combinations.  If port is not provided
	 * then getPort() will be used by default.  This list must contain at 
	 * least one valid host other it would not be possible to do a ring describe.
	 * @return
	 */
	String getSeeds();

	/**
	 * Return a list of Host objects from the list of seeds returned by getSeeds().
	 * This list must contain at least one valid host other it would not be 
	 * possible to do a ring describe.
	 * @return
	 */
	List<Host> getSeedHosts();
	
	/**
	 * Data port to be used when no port is specified to a list of seeds or
	 * when doing a ring describe since the ring describe does not include 
	 * a host
	 * @return
	 */
	int getPort();

	/**
	 * Maximum number of connections to allocate for a single host's pool
	 * @return
	 */
	int getMaxConnsPerHost();

	/**
	 * Start and interval delay trying to discover new nodes.
	 * @return
	 */
	int getAutoDiscoveryDelay();

	/**
	 * Prefix filter to apply when doing a ring describe.  This filter is used
	 * to remove nodes in other regions.
	 * 
	 * TODO: List of filters
	 * @return
	 */
	String getRingIpFilter();

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
	 * Return the backoff strategy to use.
	 * 
	 * @see com.netflix.astyanax.connectionpool.RetryBackoffStrategy
	 * @return
	 */
	RetryBackoffStrategy getRetryBackoffStrategy();

	/**
	 * Factory that creates the connection pool for a keyspace
	 * @return
	 */
	ConnectionPoolFactory getConnectionPoolFactory();
	
	/**
	 * Factory that creates the node discovery implementation to be used by 
	 * the connection pool
	 * @return
	 */
	NodeDiscoveryFactory getNodeDiscoveryFactory();
	
	/**
	 * Return strategy to use when determining which host pool to select
	 * next for an operation. 
	 * @return
	 */
	Factory<LoadBalancingStrategy> getLoadBlancingPolicyFactory();
	
	/**
	 * TODO
	 * @return
	 */
	Factory<ExhaustedStrategy> getExhaustedStrategyFactory();
	
	/**
	 * TODO
	 * @return
	 */
	Factory<FailoverStrategy> getFailoverStrategyFactory();

	/**
	 * Return clock to use when setting timestamps for column insertion and 
	 * deletion operations.
	 * @return
	 */
	Clock getClock();
	
	/**
	 * Return Monitor object to receive all connection pool notification 
	 * and counter increments. 
	 * @see com.netflix.astyanax.connectionpool.ConnectionPoolMonitor
	 * @return
	 */
	ConnectionPoolMonitor getConnectionPoolMonitor();
	
	/**
	 * Return tracer to receive operation completion notification.
	 * @see com.netflix.astyanax.connectionpool.KeyspaceTracers
	 * @return
	 */
	KeyspaceTracers getKeyspaceTracers();

	/**
	 * Return algorithm implementation for detecting when a host is down.
	 * @see com.netflix.astyanax.connectionpool.BadHostDetector
	 * @return
	 */
	BadHostDetector getBadHostDetector();

	/**
	 * Maximum amount of time to wait for a connection to free up when a connection
	 * pool is exhausted.  
	 * @return
	 */
	int getMaxTimeoutWhenExhausted();

	/**
	 * Get the max number of failover attempts
	 * @return
	 */
	int getMaxFailoverCount();

	/**
	 * Get the time to wait between failover attempts
	 * @return
	 */
	int getFailoverWaitTime();

	/**
	 * Get the max number of retries when a host pool is exhausted
	 * @return
	 */
	int getMaxExhaustedRetries();
	
	/**
	 * If returns true then debug messages will be logged
	 * @return
	 */
	public boolean isDebugEnabled();
}