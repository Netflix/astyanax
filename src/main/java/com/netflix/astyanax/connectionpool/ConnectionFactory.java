package com.netflix.astyanax.connectionpool;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;

/**
 * Factory used to create and open new connections on a host.
 * @author elandau
 *
 * @param <CL>
 */
public interface ConnectionFactory<CL> {
	Connection<CL> createConnection(HostConnectionPool<CL> pool) throws ConnectionException, OperationException;
}
