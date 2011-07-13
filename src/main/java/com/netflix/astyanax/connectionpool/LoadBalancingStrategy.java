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
