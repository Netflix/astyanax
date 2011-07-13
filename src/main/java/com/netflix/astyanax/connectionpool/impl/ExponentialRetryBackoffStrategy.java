package com.netflix.astyanax.connectionpool.impl;

import java.util.Random;

import com.netflix.astyanax.connectionpool.RetryBackoffStrategy;

public class ExponentialRetryBackoffStrategy implements RetryBackoffStrategy{
	private final int max;
	private final long timeslot;
	private final long suspendTime;
	
	public ExponentialRetryBackoffStrategy(int max, int timeslot, int suspendTime) {
		this.max = (int) Math.pow(2, max);
		this.timeslot = timeslot;
		this.suspendTime = suspendTime;
	}
	
	@Override
	public Instance createInstance() {
		return new RetryBackoffStrategy.Instance() {
			private int c = 1;
			private boolean isSuspended = false;
			
			@Override
			public long nextDelay() {
				if (isSuspended) {
					isSuspended = false;
					return suspendTime;
				}
				
				c *= 2;
				if (c > max) 
					c = max;
				
				return new Random().nextInt(c) * timeslot;
			}

			@Override
			public void suspend() {
				isSuspended = true;
			}
		};
	}
}
