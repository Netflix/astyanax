package com.netflix.astyanax.connectionpool;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * Interface for algorithm to detect when a host is considered down.
 * Once a host is considered to be down it will be added to the retry service
 * @author elandau
 *
 */
public interface BadHostDetector {
	/**
	 * Check if the host is down given the exception that occurred.  
	 * 
	 * @param host
	 * @param e	Exception that caused a connection to fail.
	 * @return
	 */
	public boolean checkFailure(Host host, ConnectionException e);
}
