package com.netflix.astyanax.connectionpool.impl;

import com.netflix.astyanax.Execution;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.UnknownException;
import com.netflix.astyanax.thrift.ThriftConverter;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ExecutionHelper {
    public static<R> OperationResult<R> blockingExecute(Execution<R> execution) throws ConnectionException {
        Future<OperationResult<R>> future = execution.executeAsync();
        try {
            return future.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UnknownException(e);
        }
        catch (ExecutionException e) {
            throw ThriftConverter.ToConnectionPoolException(e);
        }
    }

    private ExecutionHelper() {
    }
}
