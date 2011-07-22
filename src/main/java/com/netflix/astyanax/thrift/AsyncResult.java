package com.netflix.astyanax.thrift;

class AsyncResult<R>
{
    final R             result;
    final Exception     exception;

    AsyncResult(R result, Exception exception) {
        this.result = result;
        this.exception = exception;
    }
}
