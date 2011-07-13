package com.netflix.astyanax.connectionpool.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.Lists;
import com.netflix.astyanax.connectionpool.ConnectionFactory;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.LoadBalancingStrategy;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NoAvailableHostsException;

/**
 * Load balancing policy that sorts the list of active hosts based on an 
 * externally provided comparator and returns the first element in the 
 * list.
 * 
 * @author elandau
 *
 */
public class SortedLoadBalancingStrategy implements LoadBalancingStrategy {
	private Comparator<HostConnectionPool<?>> comparator;
	private final ConnectionPoolConfiguration config;
	
	public SortedLoadBalancingStrategy(ConnectionPoolConfiguration config, 
			Comparator<HostConnectionPool<?>> comparator) {
		this.comparator = comparator;
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
		
	    List<HostConnectionPool<CL>> vals = Lists.newArrayList(hosts);
	    Collections.sort(vals, comparator);
	    return vals.get(0);
	}

}
