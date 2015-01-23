package com.netflix.astyanax.contrib.valve;

import java.util.concurrent.atomic.AtomicLong;

public class TimeWindowValve {

    public static enum RequestStatus { 
        OverQuota, PastWindow, Permitted 
    };
    
    final AtomicLong limitValve;
    final Long startWindow; 
    final Long endWindow; 
    
    public TimeWindowValve(final Long limit, final Long startMilliseconds, final long windowMilliseconds) {
        
        limitValve = new AtomicLong(limit);
        startWindow = startMilliseconds;
        endWindow = startWindow + windowMilliseconds;
    }
    
    
    public RequestStatus decrementAndCheckQuota() {
       
        long currentTime = System.currentTimeMillis(); 
        if (currentTime > endWindow) {
            return RequestStatus.PastWindow;
        }
        
        if (limitValve.get() <= 0) {
            return RequestStatus.OverQuota; // this valve is done. no more requests
        }
        
        long value = limitValve.decrementAndGet();
        if (value < 0) {
            return RequestStatus.OverQuota;
        } else {
            return RequestStatus.Permitted;
        }
    }
}
