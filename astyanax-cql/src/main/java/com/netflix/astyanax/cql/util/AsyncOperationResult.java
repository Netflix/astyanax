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
package com.netflix.astyanax.cql.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.connectionpool.OperationResult;

/**
 * Impl for istenableFuture<OperationResult<V>> that wraps the {@link ResultSetFuture} from java driver for async operations. 
 * 
 * @author poberai
 *
 * @param <V>
 */
public abstract class AsyncOperationResult<V> implements ListenableFuture<OperationResult<V>> {

	private ResultSetFuture rsFuture; 

	public AsyncOperationResult(ResultSetFuture rsFuture) {
		this.rsFuture = rsFuture;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return rsFuture.cancel(mayInterruptIfRunning);
	}

	@Override
	public boolean isCancelled() {
		return rsFuture.isCancelled();
	}

	@Override
	public boolean isDone() {
		return rsFuture.isDone();
	}

	@Override
	public OperationResult<V> get() throws InterruptedException, ExecutionException {
		return getOperationResult(rsFuture.get());
	}

	public OperationResult<V> getUninterruptably() throws InterruptedException, ExecutionException {
		return getOperationResult(rsFuture.getUninterruptibly());
	}

	@Override
	public OperationResult<V> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		return getOperationResult(rsFuture.get(timeout, unit));
	}

	@Override
	public void addListener(Runnable listener, Executor executor) {
		rsFuture.addListener(listener, executor);
	}

	public abstract OperationResult<V> getOperationResult(ResultSet rs);
}
