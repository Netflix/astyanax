package com.netflix.astyanax.connectionpool.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.NodeDiscovery;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.model.TokenRange;

/**
 * Re-discover the ring on a fixed interval to identify new nodes or changes
 * to the ring tokens.
 * 
 * TODO: Support multiple filters
 * 
 * @author elandau
 *
 * @param <CL>
 */
public class RingDescribeNodeAutoDiscoveryImpl implements NodeDiscovery {
	private final ConnectionPool<?> connectionPool;
	private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
	private final ConnectionPoolConfiguration config;
	private final int interval;
	private final Keyspace keyspace;
	
	public RingDescribeNodeAutoDiscoveryImpl(ConnectionPoolConfiguration config, 
			Keyspace keyspace,
			ConnectionPool<?> connectionPool) {
		this.connectionPool = connectionPool;
		this.interval = config.getAutoDiscoveryDelay()*1000;
		this.config = config;
		this.keyspace = keyspace;
	}

	/**
	 * 
	 */
	@Override
	public void start() {
		update();
		
		executor.schedule(new Runnable() {
			@Override
			public void run() {
				update();
			}
		}, interval, TimeUnit.MILLISECONDS);
	}
	
	@Override
	public void shutdown() {
		executor.shutdown();
	}
	
	private void update() {
		try {
			connectionPool.setHosts(getHosts());
		} catch (ConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Return a mapping of token to a list of hosts.  The hosts are in ring
	 * order so the first host in the list is the primary owner of the 
	 * token range.
	 * @return
	 * @throws ConnectionException
	 * @throws OperationException 
	 */
	protected Map<String, List<Host>> getHosts() throws ConnectionException, OperationException {
		// Get list of Token->Hosts from the ring
		List<TokenRange> ranges = keyspace.describeRing();
		
		String filter = config.getRingIpFilter();
		int port = config.getPort();
		
		// Convert to a map of Token to hosts that match the filter.
		Map<String, List<Host>> hosts = new HashMap<String, List<Host>>();
		for (TokenRange range : ranges) {
			List<Host> partition = new ArrayList<Host>();
			for (String endpoint : range.getEndpoints()) {
				if (filter == null || endpoint.startsWith(filter)) {
					partition.add(new Host(endpoint, port));
				}
			}
			if (!partition.isEmpty()) {
				hosts.put(range.getStartToken(), partition);
			}
			else {
				// TODO: Should we keep an empty list or log an error?
			}
		}
		return hosts;
	}
}
