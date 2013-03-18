package com.netflix.astyanax;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * Notification interface of success or failures executing keyspace operations.
 * 
 * @author elandau
 * 
 */
public interface CassandraOperationTracer {
    CassandraOperationTracer start();

    void success();

    void failure(ConnectionException e);
}
