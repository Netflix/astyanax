package com.netflix.astyanax.connectionpool.impl;

import com.netflix.astyanax.Clock;

public class MillisecondsClock implements Clock {

	@Override
	public long getCurrentTime() {
		return System.currentTimeMillis();
	}
}
