package com.netflix.astyanax.retry;

import com.netflix.astyanax.util.StringUtils;

public class BoundedExponentialBackoff extends ExponentialBackoff {
	
	private final int maxSleepTimeMs;
	
	public BoundedExponentialBackoff(int baseSleepTimeMs, int maxSleepTimeMs, int max) {
		super(baseSleepTimeMs, max);
		this.maxSleepTimeMs = maxSleepTimeMs;
	}

	public long getSleepTimeMs() {
		return Math.max(maxSleepTimeMs, super.getSleepTimeMs());
	}
	
	public String toString() {
		return StringUtils.joinClassAttributeValues(this, "BoundedExponentialBackoff", BoundedExponentialBackoff.class);
	}
}
