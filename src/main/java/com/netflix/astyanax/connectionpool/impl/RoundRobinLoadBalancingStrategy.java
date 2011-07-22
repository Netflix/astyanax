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
package com.netflix.astyanax.connectionpool.impl;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterables;
import com.netflix.astyanax.connectionpool.ConnectionFactory;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.LoadBalancingStrategy;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NoAvailableHostsException;

/**
 * LoadBalancing strategy that returns host pools using round robin over the
 * list of active hosts.
 * @author elandau
 *
 */
public class RoundRobinLoadBalancingStrategy implements LoadBalancingStrategy {
	private final ConnectionPoolConfiguration config;
	private final AtomicInteger position = new AtomicInteger(0);
	
	public RoundRobinLoadBalancingStrategy(ConnectionPoolConfiguration config) {
		this.config = config;
	}
	
	@Override
	public <CL> HostConnectionPool<CL> createHostPool(Host host,
			ConnectionFactory<CL> factory) {
		return new SimpleHostConnectionPool<CL>(host, factory, config.getConnectionPoolMonitor(), config.getMaxConnsPerHost());
	}

	@Override
	public <CL> HostConnectionPool<CL> selectHostPool(
			Collection<HostConnectionPool<CL>> hosts)
			throws ConnectionException {
		if (hosts.isEmpty()) 
			throw new NoAvailableHostsException("No hosts available to perform host operation");
		
		int max = hosts.size();
		int cursor = position.getAndIncrement();
		return Iterables.get(hosts, cursor % max);
	}
}
