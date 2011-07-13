package com.netflix.astyanax.connectionpool.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.lang.time.StopWatch;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.netflix.astyanax.connectionpool.BadHostDetector;
import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.ConnectionFactory;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.ExhaustedStrategy;
import com.netflix.astyanax.connectionpool.FailoverStrategy;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.HostRetryService;
import com.netflix.astyanax.connectionpool.LoadBalancingStrategy;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.HostRetryService.ReconnectCallback;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.exceptions.TimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.TransportException;

/**
 * Connection pool that keeps a pool of connections per host and then uses 
 * a load balancing strategy to select a host from the pool from which to 
 * borrow a connection.
 * 
 * @author elandau
 *
 * @param <CL>
 */
public class HostPartitionConnectionPoolImpl<CL> implements ConnectionPool<CL> {

	protected final NonBlockingHashMap<Host, HostConnectionPool<CL>> foundHosts;
	protected final NonBlockingHashMap<Host, HostConnectionPool<CL>> activeHosts;
	protected final ConnectionPoolConfiguration config;
	protected final ConnectionFactory<CL> factory;
	protected final ConnectionPoolMonitor monitor;
	protected final HostRetryService retryService;
	protected final BadHostDetector badHostDetector;
	protected final FailoverStrategy failoverStrategy;
	protected final ExhaustedStrategy exhaustedStrategy;
	protected LoadBalancingStrategy loadBalancingStrategy;
	
	public HostPartitionConnectionPoolImpl(ConnectionPoolConfiguration config, ConnectionFactory<CL> factory) {
		this.foundHosts = new NonBlockingHashMap<Host, HostConnectionPool<CL>>();
		this.activeHosts = new NonBlockingHashMap<Host, HostConnectionPool<CL>>();
		this.config = config;
		this.factory = factory;
		this.retryService = new ThreadedRetryService<CL>(config.getRetryBackoffStrategy(), factory);
		this.loadBalancingStrategy = config.getLoadBlancingPolicyFactory().createInstance(this.config);
		this.exhaustedStrategy = config.getExhaustedStrategyFactory().createInstance(this.config);
		this.failoverStrategy = config.getFailoverStrategyFactory().createInstance(this.config);
		this.monitor = config.getConnectionPoolMonitor();
		this.badHostDetector = config.getBadHostDetector();
	}
	
	@Override
	public void start() {
		for (Host host : config.getSeedHosts()) {
			addHost(host);
		}
	}
	
	@Override
	public <R> Connection<CL> borrowConnection(Operation<CL, R> op)
			throws ConnectionException, OperationException {
		StopWatch sw = new StopWatch();
		int retryCount = 0;
		while (true) {
			HostConnectionPool<CL> pool = null;
			try {
				pool = this.loadBalancingStrategy.selectHostPool(activeHosts.values());
				Connection<CL> connection = pool.borrowConnection(config.getSocketTimeout());
				this.monitor.incConnectionBorrowed(pool.getHost(), sw.getTime());
				return connection;
			}
			catch (TransportException e) {
				if (this.badHostDetector.checkFailure(pool.getHost(), e)) {
					this.markHostAsDown(pool, e);
				}
			}
			catch (TimeoutException e) {
				if (++retryCount < this.exhaustedStrategy.getMaxRetries()) {
					this.monitor.incBorrowRetry();
					// TODO
				}
				else {
					throw new TimeoutException("");
				}
			}
		}
	}

	@Override
	public void returnConnection(Connection<CL> connection) {
		this.monitor.incConnectionReturned(connection.getHostConnectionPool().getHost());
		connection.getHostConnectionPool().returnConnection(connection);
		
		if (!connection.isOpen()) {
			if (null != connection.getLastException() &&
				badHostDetector.checkFailure(connection.getHostConnectionPool().getHost(), 
						connection.getLastException())) {
				this.markHostAsDown(connection.getHostConnectionPool(), connection.getLastException());
			}
		}
	}
	

	@Override
	public void addHost(Host host) {
		HostConnectionPool<CL> pool = loadBalancingStrategy.createHostPool(host, factory);
		if (null == this.foundHosts.putIfAbsent(host, pool)) {
			this.monitor.onHostAdded(host, new ImmutableHostConnectionPool<CL>(pool));
			this.activeHosts.putIfAbsent(host, pool);
		}
	}

	@Override
	public void removeHost(Host host) {
		HostConnectionPool<CL> pool = foundHosts.remove(host);
		if (pool != null) {
			this.monitor.onHostRemoved(host);
			pool.shutdown();
		}
		this.activeHosts.remove(host);
	}
	
	protected void markHostAsDown(HostConnectionPool<CL> pool, Exception reason) {
		if (this.activeHosts.remove(pool.getHost(), pool)) {
			this.monitor.onHostDown(pool.getHost(), reason);
			pool.shutdown();
			
			this.retryService.addHost(pool.getHost(), new ReconnectCallback() {
				@Override
				public void onReconnected(Host host) {
					reactivateHost(host);
				}
			});
		}
	}
	
	protected void reactivateHost(Host host) {
		HostConnectionPool<CL> pool = loadBalancingStrategy.createHostPool(host, factory);
		this.monitor.onHostReactivated(host, new ImmutableHostConnectionPool<CL>(pool));
		
		foundHosts.put(host, pool);
		activeHosts.put(host, pool);
	}
	
	@Override
	public void setHosts(Map<String, List<Host>> ring) {
		// Temporary list of hosts to remove.  Any host not in the new ring
		// will be removed
		Set<Host> hostsToRemove = new HashSet<Host>();
		for (Entry<Host, HostConnectionPool<CL>> h : foundHosts.entrySet()) {
			hostsToRemove.add(h.getKey());
		}
		
		// Add new hosts.  It is only necessary to take the first host for a token
		// because other hosts in the list are replicas.
		for (Map.Entry<String, List<Host>> entry : ring.entrySet()) {
			List<Host> hosts = entry.getValue();
			if (hosts != null) {
				Host host = entry.getValue().iterator().next();
				HostConnectionPool<CL> pool = new SimpleHostConnectionPool<CL>(host, factory, config.getMaxConnsPerHost());
				if (null == foundHosts.putIfAbsent(host, pool)) {
					this.monitor.onHostAdded(host, new ImmutableHostConnectionPool<CL>(pool));
					activeHosts.put(host, pool);
				}
				hostsToRemove.remove(host);
			}
		}
		
		// Remove any hosts that are no longer in the ring
		for (Host host : hostsToRemove) {
			removeHost(host);
		}
	}

	@Override
	public void shutdown() {
		this.retryService.shutdown();
		for (Entry<Host, HostConnectionPool<CL>> pool : foundHosts.entrySet()) {
			pool.getValue().shutdown();
		}
	}

	@Override
	public <R> OperationResult<R> executeWithFailover(
			Operation<CL, R> op) throws ConnectionException, OperationException {
		int retryCount = 0;
		while (true) {
			// First try to get a connection
			Connection<CL> connection = null;
			try {
				connection = borrowConnection(op);
			}
			catch (TimeoutException e) {
				throw e;
			}

			// Now try to execute
			try {
				return connection.execute(op);
			}
			catch (ConnectionException e) {
				if (!e.isRetryable()) 
					throw e;
			}
			finally {
				returnConnection(connection);
			}
			
			// Apply the retry strategy
			if (++retryCount < this.failoverStrategy.getMaxRetries()) {
				this.monitor.incFailover();
				try {
					if (this.failoverStrategy.getWaitTime() > 0) {
						Thread.sleep(this.failoverStrategy.getWaitTime());
					}
				} catch (InterruptedException e) {
					throw new TimeoutException("Interrupted sleeping between retries");
				}
			}
			else {
				throw new TimeoutException("Operation failed too many times");
			}
		}
	}
}
