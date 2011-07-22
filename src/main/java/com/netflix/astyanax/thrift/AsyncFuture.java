package com.netflix.astyanax.thrift;

import com.netflix.astyanax.connectionpool.OperationResult;

import org.apache.thrift.async.TAsyncMethodCall;

import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

public class AsyncFuture<R, A extends TAsyncMethodCall, T extends AbstractAsyncOperationImpl<R, A>> extends
        FutureTask<OperationResult<R>> {
    private final T operation;

    public static<R, A extends TAsyncMethodCall, T extends AbstractAsyncOperationImpl<R, A>> Future<OperationResult<R>>
        make(T operation)
    {
        return new AsyncFuture<R, A, T>(operation);
    }

    public AsyncFuture(T operation) {
        super(operation);
        this.operation = operation;
        operation.start();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        operation.cancel();
        return super.cancel(mayInterruptIfRunning);
    }
}
