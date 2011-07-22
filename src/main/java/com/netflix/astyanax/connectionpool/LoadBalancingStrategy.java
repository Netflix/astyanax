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

import java.util.Collection;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * Interface for an algorithm that load balances hosts (i.e. selects hosts) based
 * on certain critera/algorithm.
 * @author elandau
 *
 */
public interface LoadBalancingStrategy {
	/**
	 * Create a host's connection pool implementation.  
	 * @param <CL>
	 * @param host
	 * @param factory
	 * @return
	 */
	<CL> HostConnectionPool<CL> createHostPool(Host host, ConnectionFactory<CL> factory);
	
	/**
	 * Given a collection of all available hosts select a host's connection pool
	 * using the load balancing algorithm
	 * 
	 * @param <CL>
	 * @param hosts
	 * @return
	 * @throws ConnectionException
	 */
	<CL> HostConnectionPool<CL> selectHostPool(Collection<HostConnectionPool<CL>> hosts) throws ConnectionException;
}
