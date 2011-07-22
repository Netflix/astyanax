package com.netflix.astyanax.connectionpool;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

public interface ExecuteWithFailover<CL, R> {
    public Host getHost();

    public OperationResult<R> tryOperation(Operation<CL, R> operation) throws ConnectionException;

    public void informException(ConnectionException e) throws ConnectionException;
}
