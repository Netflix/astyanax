package com.netflix.astyanax.connectionpool.impl;

import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.OperationResult;

public class OperationResultImpl<R> implements OperationResult<R> {
	
	private final Host host;
	private final R result;
	private final long latency;
	
	public OperationResultImpl(Host host, R result, long latency) {
		this.host = host;
		this.result = result;
		this.latency = latency;
	}
	
	@Override
	public Host getHost() {
		return this.host;
	}

	@Override
	public R getResult() {
		return this.result;
	}

	@Override
	public long getLatency() {
		return this.latency;
	}

}
