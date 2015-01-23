package com.netflix.astyanax.contrib.valve;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.astyanax.contrib.valve.TimeWindowValve.RequestStatus;

public class RollingTimeWindowValve {

    private final AtomicReference<InnerState> currentRef = new AtomicReference<InnerState>(null);
    
    
    private final AtomicLong ratePerSecond = new AtomicLong(0L);
    private final AtomicInteger numBuckets = new AtomicInteger(0);
    private final AtomicBoolean valveCheckDisabled = new AtomicBoolean(false);
    
    public RollingTimeWindowValve(long rps, int nBuckets) {

        ratePerSecond.set(rps);
        numBuckets.set(nBuckets);
        
        currentRef.set(new InnerState(System.currentTimeMillis()));
    }
    
    public void setRatePerSecond(Long newRps) {
        ratePerSecond.set(newRps);   
    }
    
    public void setNumBuckets(int newBuckets) {
        numBuckets.set(newBuckets);   
    }
    
    public void disableValveCheck() {
        valveCheckDisabled.set(true);
    }
    
    public void enableValveCheck() {
        valveCheckDisabled.set(false);
    }

    public boolean decrementAndCheckQuota() {
        
        if (valveCheckDisabled.get()) {
            return true;
        }
        
        InnerState currentState = currentRef.get(); 
        
        TimeWindowValve currentWindow = currentState.window;
        RequestStatus status = currentWindow.decrementAndCheckQuota();
        
        if (status == RequestStatus.Permitted) {
            return true;
        }
        
        if (status == RequestStatus.OverQuota) {
            return false;
        }
        
        if (status == RequestStatus.PastWindow) {
            
            InnerState nextState = new InnerState(System.currentTimeMillis());
            boolean success = currentRef.compareAndSet(currentState, nextState);
            if (success) {
                //System.out.println("FLIP");
            }
            // Try one more time before giving up
            return (currentRef.get().window.decrementAndCheckQuota() == RequestStatus.Permitted);
        }
        
        return false;
    }
    
    private class InnerState {
        
        private final String id = UUID.randomUUID().toString();
        private final Long startTime;
        
        private final TimeWindowValve window;
        
        private InnerState(Long startWindowMillis) {
            
            startTime = startWindowMillis;
            int nBuckets = numBuckets.get();
            
            long rateForWindow = ratePerSecond.get()/nBuckets;
            long windowMillis = 1000/nBuckets;
            
            window = new TimeWindowValve(rateForWindow, startWindowMillis, windowMillis);
        }
        
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + ((startTime == null) ? 0 : startTime.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            
            if (this == obj) return true;
            if (obj == null) return false;
            
            if (getClass() != obj.getClass()) return false;
            
            InnerState other = (InnerState) obj;
            boolean equals = true; 
            equals &= (id == null) ? other.id == null : id.equals(other.id);
            equals &= (startTime == null) ? other.startTime == null : startTime.equals(other.startTime);
            return equals;
        }

        @Override
        public String toString() {
            return id.toString();
        }
    }
}
