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

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ConsistencyLevel;

public interface CounterMutation<K, C> extends Execution<Void> {
	/**
	 * Set the consistency level for this query
	 * @param consistencyLevel
	 */
	CounterMutation<K,C> setConsistencyLevel(ConsistencyLevel consistencyLevel);
	
	/**
	 * Set the timeout for this operation.  
	 * @param timeout	In milliseconds
	 */
	CounterMutation<K,C> setTimeout(long timeout);
	
}
