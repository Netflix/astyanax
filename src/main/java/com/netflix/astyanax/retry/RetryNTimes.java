package com.netflix.astyanax.retry;

import com.netflix.astyanax.util.StringUtils;

public class RetryNTimes implements RetryPolicy {
	private final int max;
	private int attempts; 
	
	public RetryNTimes(int max) {
		this.max = max;
		this.attempts = 0;
	}
	
	@Override
	public void begin() {
		this.attempts = 0;
	}

	@Override
	public void success() {
	}

	@Override
	public void failure(Exception e) {
	}

	@Override
	public boolean allowRetry() {
		if (max == -1 || attempts < max) {
			attempts++;
			return true;
		}
		return false;
	}

	@Override
	public int getAttemptCount() {
		return attempts;
	}

	@Override
	public RetryPolicy duplicate() {
		return new RetryNTimes(max);
	}

	public String toString() {
		return StringUtils.joinClassAttributeValues(this, "RetryNTimes", RetryNTimes.class);
	}
}
