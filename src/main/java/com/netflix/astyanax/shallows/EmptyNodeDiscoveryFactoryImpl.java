package com.netflix.astyanax.shallows;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.NodeDiscovery;
import com.netflix.astyanax.connectionpool.NodeDiscoveryFactory;

public class EmptyNodeDiscoveryFactoryImpl implements NodeDiscoveryFactory {

	private static final EmptyNodeDiscoveryFactoryImpl instance = new EmptyNodeDiscoveryFactoryImpl();
	
	private EmptyNodeDiscoveryFactoryImpl() {
		
	}
	
	public static EmptyNodeDiscoveryFactoryImpl get() {
		return instance;
	}
	
	@Override
	public <CL> NodeDiscovery createNodeDiscovery(
			ConnectionPoolConfiguration config, Keyspace keypsace,
			ConnectionPool<CL> connectionPool) {
		return new NodeDiscovery() {
			@Override
			public void start() {
			}

			@Override
			public void shutdown() {
			}
		};
	}
}
