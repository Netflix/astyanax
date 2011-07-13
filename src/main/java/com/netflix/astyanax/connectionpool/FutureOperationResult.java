package com.netflix.astyanax.connectionpool;

import java.util.concurrent.Future;

public interface FutureOperationResult<R> extends OperationResult<R>, Future<R> {
}
