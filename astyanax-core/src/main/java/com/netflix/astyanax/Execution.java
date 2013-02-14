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
package com.netflix.astyanax;

import java.util.concurrent.Callable;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Interface for an operation that can be executed on the cluster.
 *
 * @param <R> - The return type for the concrete operation
 * 
 * @author elandau
 */
public interface Execution<R>  {
    /**
     * Block while executing the operations
     *
     * @return Result object that wraps the actual result and provides information about
     *          how the operation was executed.
     * @throws ConnectionException
     */
    OperationResult<R> execute() throws ConnectionException;

    /**
     * Return a future to the operation. The operation will most likely be
     * executed in a separate thread where both the connection pool logic as
     * well as the actual operation will be executed.
     *
     * @return A listenable future 
     * @throws ConnectionException
     */
    ListenableFuture<OperationResult<R>> executeAsync() throws ConnectionException;
}
