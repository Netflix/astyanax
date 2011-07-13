package com.netflix.astyanax.connectionpool.impl;

import java.util.Collection;
import java.util.Random;
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
		return new SimpleHostConnectionPool<CL>(host, factory, config.getMaxConnsPerHost());
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
