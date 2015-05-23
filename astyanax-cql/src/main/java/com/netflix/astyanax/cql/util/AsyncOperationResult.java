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
