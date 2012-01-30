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

import java.util.concurrent.ExecutorService;

import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;

/**
 * Interface defining all astyanax API configuration parameters.
 * 
 * @author elandau
 *
 */
public interface AstyanaxConfiguration {
	/**
	 * TODO
	 * @return
	 */
	RetryPolicy getRetryPolicy();
	
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
	 * Return the maximum number of allows async threads to executeAsync()
	 * @return
	 */
	ExecutorService getAsyncExecutor();

	/**
	 * Fixed delay for node disocvery refresh
	 */
	int getDiscoveryDelayInSeconds();

	/**
	 * Get type of node discovery to perform
	 * @return
	 */
	NodeDiscoveryType getDiscoveryType();

	/**
	 * Type of connection pool to use for this instance
	 * @return
	 */
	ConnectionPoolType getConnectionPoolType();
}
