package com.netflix.astyanax;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import java.util.concurrent.Future;

/**
 * Interface for an operation that can be executed on the cluster.  
 * 
 * @author elandau
 *
 * @param <R>
 */
public interface Execution<R> {
	/**
	 * Block while executing the operations
	 * 
	 * @return
	 * @throws ConnectionException
	 */
	OperationResult<R> execute() throws ConnectionException;

	/**
	 * Return a future to the operation.  The operation will most likely be 
	 * executed in a separate thread where both the connection pool logic
	 * as well as the actual operation will be executed.
	 * 
	 * @return
	 * @throws ConnectionException
	 */
	Future<OperationResult<R>> executeAsync() throws ConnectionException;
}
