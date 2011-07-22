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
 * Abstraction for a service used to retry connecting to a host.  Hosts are placed
 * in this service after they have been identified to be down and were removed
 * from the list of available hosts in the connection pool.
 * @author elandau
 *
 * @param <CL>
 */
public interface HostRetryService {
	public static interface ReconnectCallback {
		public void onReconnected(Host host);
	}
	
	/**
	 * Shut down the reconnect service and any attempts to reconnect
	 */
	void shutdown();
	
	/**
	 * Add a host to the service
	 * @param host
	 */
	void addHost(Host host, ReconnectCallback callback);

	/**
	 * Stop retrying the host
	 * @param host
	 */
	void removeHost(Host host);
}
