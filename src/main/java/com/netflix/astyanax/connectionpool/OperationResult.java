package com.netflix.astyanax.connectionpool;


public interface OperationResult<R> {
	/**
	 * Get the host on which the operation was performed
	 * @return
	 */
	Host getHost();
	
	/**
	 * Get the result data
	 * @return
	 */
	R getResult();
	
	/**
	 * Return the length of time to perform the operation.  Does not include
	 * connection pool overhead.
	 * @return
	 */
	long getLatency();
}
