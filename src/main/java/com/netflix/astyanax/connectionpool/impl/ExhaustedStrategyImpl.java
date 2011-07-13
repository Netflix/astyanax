package com.netflix.astyanax.connectionpool.impl;

import com.netflix.astyanax.connectionpool.ExhaustedStrategy;

public class ExhaustedStrategyImpl implements ExhaustedStrategy {
	private int maxRetries;
	private int waitTime;
	
	public ExhaustedStrategyImpl(int maxRetries, int waitTime) {
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
