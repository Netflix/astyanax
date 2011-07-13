package com.netflix.astyanax.connectionpool;

/**
 * Interface for a module that periodically updates the nodes in a connection
 * pool.  
 * 
 * @author elandau
 *
 */
public interface NodeDiscovery {
	void start();
	
	void shutdown();
}
