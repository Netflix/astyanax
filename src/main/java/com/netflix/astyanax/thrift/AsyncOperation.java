package com.netflix.astyanax.thrift;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.async.TAsyncMethodCall;

public interface AsyncOperation<R, A extends TAsyncMethodCall> {
    public void     startOperation(Cassandra.AsyncClient client) throws ConnectionException;

    public R        finishOperation(A response) throws ConnectionException;

    public void     trace(OperationResult<R> result);
}
