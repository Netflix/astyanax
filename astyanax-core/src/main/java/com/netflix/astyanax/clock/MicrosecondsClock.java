package com.netflix.astyanax.clock;

import com.netflix.astyanax.Clock;

/**
 * 
 * @author Patricio Echague (pechague@gmail.com)
 */
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
