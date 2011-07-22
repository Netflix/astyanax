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
