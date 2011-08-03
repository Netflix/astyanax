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
package com.netflix.astyanax.connectionpool.impl;

import com.netflix.astyanax.Execution;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.UnknownException;

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
        	if (e.getCause() instanceof ConnectionException) {
        		throw (ConnectionException)e.getCause();
        	}
            throw new UnknownException(e);
        }
    }

    private ExecutionHelper() {
    }
}
