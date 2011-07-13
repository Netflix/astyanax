package com.netflix.astyanax.connectionpool;

import java.math.BigInteger;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;

/**
 * Callback interface to perform an operation on a client associated
 * with a connection pool's connection resource
 * 
 * @author elandau
 *
 * @param <C>
 * @param <R>
 */
public interface Operation<CL, R> {
	/**
	 * Execute the operation on the client object and return the results
	 * @param client
	 * @return
	 * @throws OperationException 
	 * @throws NetflixCassandraException 
	 */
	R execute(CL client) throws ConnectionException, OperationException;

	/**
	 * Return the unique key on which the operation is performed or null
	 * if the operation is performed on multiple keys.
	 * @return
	 */
	BigInteger getKey();

	/**
	 * Return keyspace for this operation.  Return null if using the current
	 * keyspace, or a keyspace is not needed for the operation.
	 * @return
	 */
	String getKeyspace();
}
