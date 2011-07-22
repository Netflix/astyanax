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
package com.netflix.astyanax.connectionpool;

import java.util.List;

public interface ConnectionPoolConfiguration {
	public interface Factory<T> {
		T createInstance(ConnectionPoolConfiguration config);
	}
	
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
	 * Prefix filter to apply when doing a ring describe.  This filter is used
	 * to remove nodes in other regions.
	 * 
	 * TODO: List of filters
	 * @return
	 */
	String getRingIpFilter();

	/**
	 * True if auto discovery should use a ring_describe
	 * @return
	 */
	boolean isRingDescribeEnabled();

	/**
	 * Data port to be used when no port is specified to a list of seeds or
	 * when doing a ring describe since the ring describe does not include 
	 * a host
	 * @return
	 */
	int getPort();

	/**
	 * Start and interval delay trying to discover new nodes.
	 * @return
	 */
	int getAutoDiscoveryDelay();

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
	 * Maximum number of connections to allocate for a single host's pool
	 * @return
	 */
	int getMaxConnsPerHost();


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
	 * Return the backoff strategy to use.
	 * 
	 * @see com.netflix.astyanax.connectionpool.RetryBackoffStrategy
	 * @return
	 */
	RetryBackoffStrategy getRetryBackoffStrategy();

	/**
	 * Return Monitor object to receive all connection pool notification 
	 * and counter increments. 
	 * @see com.netflix.astyanax.connectionpool.ConnectionPoolMonitor
	 * @return
	 */
	ConnectionPoolMonitor getConnectionPoolMonitor();
	
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
	 * Socket connect/read/write timeout
	 * @return
	 */
	int getSocketTimeout();

}
