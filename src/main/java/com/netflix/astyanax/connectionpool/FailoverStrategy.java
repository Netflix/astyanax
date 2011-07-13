package com.netflix.astyanax.connectionpool;

/**
 * Strategy to employ when an operation failed due to a failure in the connection
 * pool. 
 * 
 * @author elandau
 *
 */
public interface FailoverStrategy {
	/**
	 * Number of times to retry
	 * @return
	 */
	int getMaxRetries();
	
	/**
	 * Time to wait between retries
	 * @return
	 */
	int getWaitTime();
}
