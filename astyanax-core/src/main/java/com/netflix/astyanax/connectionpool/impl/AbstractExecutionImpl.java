/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.astyanax.connectionpool.impl;

import com.netflix.astyanax.Execution;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.IsRetryableException;
import com.netflix.astyanax.retry.RetryPolicy;

/**
 * Abstract impl that repeatedly executes while consulting a {@link RetryPolicy}
 * 
 * @author elandau
 *
 * @param <R>
 * 
 * @see {@link RetryPolicy}
 */
public abstract class AbstractExecutionImpl<R> implements Execution<R> {
    public OperationResult<R> executeWithRetry(RetryPolicy retry) throws ConnectionException {
        ConnectionException lastException = null;
        retry.begin();
        do {
            try {
                return execute();
            }
            catch (ConnectionException ex) {
                if (ex instanceof IsRetryableException)
                    lastException = ex;
                else
                    throw ex;
            }
        } while (retry.allowRetry());

        throw lastException;
    }
}
