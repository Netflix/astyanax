package com.netflix.astyanax.connectionpool;

/**
 * Monitoring interface to receive notification of pool events.  A concrete
 * monitor will make event stats available to a monitoring application and
 * may also log events to a log file.
 * @author elandau
 */
public interface ConnectionPoolMonitor {
	/**
	 * Errors trying to execute an operation
	 * @param host
	 */
	void incOperationFailure(Host host, Exception e);

	/**
	 * Succeeded in executing an operation
	 * @param host
	 * @param latency
	 */
	void incOperationSuccess(Host host, long latency);
	
	// void incInvalidState(Host host);

	/**
	 * Attempt to create a connection
	 */
	void incConnectionCreated(Host host);
	
	/**
	 * Attempt to create a connection failed
	 * @param host
	 * @param e
	 */
	void incConnectionCreateFailed(Host host, Exception e);
	
	/**
	 * Incremented for each connection borrowed
	 * @param host 	Host from which the connection was borrowed	
	 * @param delay Time spent in the connection pool borrowing the connection
	 */
	void incConnectionBorrowed(Host host, long delay);

	/**
	 * Incremented for each connection returned.
	 * @param host Host to which connection is returned
	 */
	void incConnectionReturned(Host host);

	/**
	 * Timeout trying to get a connection from the pool
	 */
	void incPoolExhaustedTimeout();

	/**
	 * Timeout waiting for a response from the cluster
	 */
	void incOperationTimeout();

	/**
	 * An operation failed due to a connection error.
	 */
	void incFailover();
	
	/**
	 * A host was added and given the associated pool.  The pool is immutable
	 * and can be used to get info about the number of open connections
	 * @param host
	 * @param pool
	 */
	void onHostAdded(Host host, HostConnectionPool<?> pool);
	
	/**
	 * A host was removed from the pool.  This is usually called when a downed
	 * host is removed from the ring.
	 * @param host
	 */
	void onHostRemoved(Host host);
	
	/**
	 * A host was identified as downed.
	 * @param host
	 * @param reason  Exception that caused the host to be identified as down
	 */
	void onHostDown(Host host, Exception reason);
	
	/**
	 * A host was reactivated after being marked down
	 * @param host
	 * @param pool
	 */
	void onHostReactivated(Host host, HostConnectionPool<?> pool);

	/**
	 * There were no active hosts in the pool to borrow from. 
	 */
	void incNoHosts();
}
