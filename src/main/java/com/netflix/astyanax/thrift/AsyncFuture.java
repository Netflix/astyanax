/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.thrift;

import com.netflix.astyanax.connectionpool.OperationResult;

import org.apache.thrift.async.TAsyncMethodCall;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

public class AsyncFuture<R, A extends TAsyncMethodCall, T extends AbstractAsyncOperationImpl<R, A>> extends
        FutureTask<OperationResult<R>> {
    private final T operation;

    public static<R, A extends TAsyncMethodCall, T extends AbstractAsyncOperationImpl<R, A>> Future<OperationResult<R>>
        make(ExecutorService executorService, T operation)
    {
        return new AsyncFuture<R, A, T>(executorService, operation);
    }

    public AsyncFuture(ExecutorService executorService, T operation) {
        super(operation);
        this.operation = operation;
        executorService.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                AsyncFuture.this.operation.start();
                run();
                return null;
            }
        });
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        operation.cancel();
        return super.cancel(mayInterruptIfRunning);
    }
}
