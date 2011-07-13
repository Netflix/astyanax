package com.netflix.astyanax.connectionpool;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * Interface to an instance of a connection on a host.
 * 
 * @author elandau
 *
 * @param <CL>
 */
public interface Connection<CL> {
	/**
	 * Execute an operation on the connection and return a result
	 * 
	 * @param <R>
	 * @param op
	 * @return
	 * @throws ConnectionException 
	 */
	public <R> OperationResult<R> execute(Operation<CL, R> op) throws ConnectionException;

	/**
	 * TODO
	 * 
	 * @param <R>
	 * @param op
	 * @return
	 * @throws ConnectionException
	 */
	public <R> FutureOperationResult<R> execute(AsyncOperation<CL, R> op) throws ConnectionException;
	
	/**
	 * Shut down the connection.  isOpen() will now return false.
	 */
	public void close();

	/**
	 * Determine if the connection is open.
	 * @return
	 */
	public boolean isOpen();
	
	/**
	 * Get the parent host connection pool.
	 * @return
	 */
	public HostConnectionPool<CL> getHostConnectionPool();
	
	/**
	 * Get the last exception that caused the connection to be closed
	 * @return
	 */
	public ConnectionException getLastException();
	
	/**
	 * Open a connection
	 * @throws ConnectionException 
	 */
	void open() throws ConnectionException;
	
}
