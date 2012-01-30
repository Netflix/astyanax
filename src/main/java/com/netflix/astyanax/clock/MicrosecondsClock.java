package com.netflix.astyanax.clock;

import com.netflix.astyanax.Clock;

public class MicrosecondsClock implements Clock {

	private static final long ONE_THOUSAND = 1000L;
	  
	@Override
	public long getCurrentTime() {
	    return System.currentTimeMillis() * ONE_THOUSAND;
	}
	
	public String toString() {
		return "MicrosecondsClock";
	}

}
