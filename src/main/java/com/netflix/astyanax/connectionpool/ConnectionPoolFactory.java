package com.netflix.astyanax.connectionpool;

/**
 * TODO
 * 
 * @author elandau
 *
 */
public interface ConnectionPoolFactory {
	<CL> ConnectionPool<CL> createConnectionPool(
			ConnectionPoolConfiguration config, 
			ConnectionFactory<CL> connectionFactory);
}
