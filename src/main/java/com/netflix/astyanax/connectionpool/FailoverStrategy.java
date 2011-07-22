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
 * Strategy to employ when an operation failed due to a failure in the connection
 * pool. 
 * 
 * @author elandau
 *
 */
public interface FailoverStrategy {
	/**
	 * Number of times to retry
	 * @return
	 */
	int getMaxRetries();
	
	/**
	 * Time to wait between retries
	 * @return
	 */
	int getWaitTime();
}
