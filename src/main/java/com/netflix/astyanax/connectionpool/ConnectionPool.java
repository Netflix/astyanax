package com.netflix.astyanax.connectionpool;

import java.util.List;
import java.util.Map;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;

/**
 * Base interface for a pool of connections.  A concrete connection pool will
 * track hosts in a cluster.
 * 
 * Usage:
 * 	Connection<SomeClientImplementation> conn = null;
 * 	try {
 * 		conn = connectionPool.borrowConnection(null)
 *  	conn.execute(operation);
 *  }
 *  finally {
 *  	if (conn != null) {
 * 		 	connectionPool.returnConnection(conn);
 * 		}
 * }
 *  
 * @author elandau
 *
 *  
 * TODO:  Connection pool failover
 * TODO:  Monitoring
 * 
 * @param <CL>
 */
public interface ConnectionPool<CL> {
	/**
	 * Get an available connection from the pool.  May block if no connection
	 * is available or throw an exception if timed out.  The caller must call
	 * returnConnection() once the connection is no longer needs.  
	 * 
	 * @param token  	Optional parameter used by the connection pool to optimize
	 * 					the connection being return. 
	 * @return 
	 * @throws ConnectionException 
	 * @throws OperationException 
	 */
	<R> Connection<CL> borrowConnection(Operation<CL, R> op) throws ConnectionException, OperationException;
	
	/**
	 * Return a connection to the pool.  This must also be called for failed 
	 * connections.
	 * 
	 * @param connection
	 */
	void returnConnection(Connection<CL> connection);

	/**
	 * Add a host to the connection pool.  
	 * @param host
	 * @throws ConnectionException 
	 */
	void addHost(Host host);
	
	/**
	 * Remove a host from the connection pool.  Any pending connections will
	 * be allowed to complete
	 * @param host
	 */
	void removeHost(Host host);

	/**
	 * Sets the complete set of hosts keyed by token.
	 * @param ring
	 */
	void setHosts(Map<String, List<Host>> ring);
	
	/**
	 * Execute an operation with failover within the context of the connection pool.
	 * The operation will only fail over for connection pool errors and not 
	 * application errors.
	 * @param <R>
	 * @param op
	 * @param token
	 * @return
	 * @throws ConnectionException 
	 * @throws OperationException 
	 */
	<R> OperationResult<R> executeWithFailover(Operation<CL, R> op) throws ConnectionException, OperationException;

    <R> ExecuteWithFailover<CL, R>   newExecuteWithFailover() throws ConnectionException;

	/**
	 * Shut down the connection pool and terminate all existing connections
	 */
	void shutdown();

	/**
	 * Setup the connection pool and start any maintenance threads
	 */
	void start();

}
