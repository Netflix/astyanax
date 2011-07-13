package com.netflix.astyanax.connectionpool.impl;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.astyanax.connectionpool.FutureOperationResult;
import com.netflix.astyanax.connectionpool.Host;

public class FutureOperationResultImpl<R> implements FutureOperationResult<R> {

	private AtomicReference<OperationResultImpl<R>> result = new AtomicReference<OperationResultImpl<R>>();
	private AtomicBoolean cancelled = new AtomicBoolean(false);
	private final CountDownLatch latch = new CountDownLatch(1);
	private AtomicReference<Exception> exception = new AtomicReference<Exception>();
	
	void setResult(Host host, R result, long latency) {
		this.result.set(new OperationResultImpl<R>(host, result, latency));
		this.latch.countDown();
	}
	
	void setException(Exception e) {
		this.exception.set(e);
		this.latch.countDown();
	}
	
	@Override
	public Host getHost() {
		return this.result.get().getHost();
	}

	@Override
	public R getResult() {
		return this.result.get().getResult();
	}

	@Override
	public long getLatency() {
		return this.result.get().getLatency();
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		boolean previous = cancelled.getAndSet(true);
		if (previous == true) {
			return false;
		}
		this.latch.countDown();
		return true;
	}

	@Override
	public R get() throws InterruptedException, ExecutionException {
		this.latch.await();
		if (isCancelled()) {
			throw new CancellationException();
		}
		if (exception != null) {
			throw new ExecutionException(exception.get());
		}
		return getResult();
	}

	@Override
	public R get(long timeout, TimeUnit units) throws InterruptedException,
			ExecutionException, TimeoutException {
		if (!this.latch.await(timeout, units)) {
			if (isCancelled()) {
				throw new CancellationException();
			}
			throw new TimeoutException();
		}
		if (exception != null) {
			throw new ExecutionException(exception.get());
		}
		return getResult();
	}

	@Override
	public boolean isCancelled() {
		return this.cancelled.get();
	}

	@Override
	public boolean isDone() {
		return this.cancelled.get() || this.result.get() != null;
	}
}
