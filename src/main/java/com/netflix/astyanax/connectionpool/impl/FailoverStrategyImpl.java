package com.netflix.astyanax.connectionpool.impl;

import com.netflix.astyanax.connectionpool.FailoverStrategy;

public class FailoverStrategyImpl implements FailoverStrategy {
	private final int maxRetries;
	private final int waitTime;
	
	public FailoverStrategyImpl(int maxRetries, int waitTime) {
		this.maxRetries = maxRetries;
		this.waitTime = waitTime;
	}
	
	@Override
	public int getMaxRetries() {
		return this.maxRetries;
	}
	
	@Override
	public int getWaitTime() {
		return this.waitTime;
	}

}
