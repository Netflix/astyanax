package com.netflix.astyanax.connectionpool;

/**
 * Strategy to employ when a host's connection pool has been exhausted.
 * 
 * @author elandau
 *
 */
public interface ExhaustedStrategy {
	/**
	 * Number of hosts to retry
	 * @return
	 */
	int getMaxRetries();
	
	/**
	 * Max timeout waiting for a connection to free up in a host's pool
	 * @return
	 */
	int getWaitTime();
}
