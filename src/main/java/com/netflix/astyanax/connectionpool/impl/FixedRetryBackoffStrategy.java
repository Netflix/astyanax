package com.netflix.astyanax.connectionpool.impl;

import com.netflix.astyanax.connectionpool.RetryBackoffStrategy;

public class FixedRetryBackoffStrategy implements RetryBackoffStrategy{
	private final int timeout;
	private final int suspendTime;
	
	public FixedRetryBackoffStrategy(int timeout, int suspendTime) {
		this.timeout = timeout;
		this.suspendTime = suspendTime;
	}
	
	@Override
	public Instance createInstance() {
		return new RetryBackoffStrategy.Instance() {
			private boolean isSuspended = false;
			
			@Override
			public long nextDelay() {
				if (isSuspended) {
					isSuspended = false;
					return suspendTime;
				}
				return timeout;
			}

			@Override
			public void suspend() {
				isSuspended = true;
			}
		};
	}
}
