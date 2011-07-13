package com.netflix.astyanax.connectionpool;

import com.netflix.astyanax.Keyspace;

public interface NodeDiscoveryFactory {
	<CL> NodeDiscovery createNodeDiscovery(
			ConnectionPoolConfiguration config, 
			Keyspace keypsace,
			ConnectionPool<CL> connectionPool);
}
