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

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * Interface for algorithm to detect when a host is considered down.
 * Once a host is considered to be down it will be added to the retry service
 * @author elandau
 *
 */
public interface BadHostDetector {
	/**
	 * Check if the host is down given the exception that occurred.  
	 * 
	 * @param host
	 * @param e	Exception that caused a connection to fail.
	 * @return
	 */
	public boolean checkFailure(Host host, ConnectionException e);
}
