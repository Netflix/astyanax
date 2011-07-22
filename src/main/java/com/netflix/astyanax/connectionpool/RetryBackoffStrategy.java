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

/**
 * Strategy used to calculate how much to back off for each subsequent attempt
 * to reconnect to a downed host
 * @author elandau
 *
 */
public interface RetryBackoffStrategy {
	public interface Instance {
		/**
		 * Return the next backoff delay in the strategy
		 * @return
		 */
		long nextDelay();

		/**
		 * nextTimeout should take into account that the host is being
		 * suspended.  
		 */
		void suspend();		
	};
	
	/**
	 * Create an instance of the strategy for a single host
	 * @return
	 */
	Instance createInstance();
}
