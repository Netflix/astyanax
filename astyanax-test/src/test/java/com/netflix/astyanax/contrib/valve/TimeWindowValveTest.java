package com.netflix.astyanax.contrib.valve;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.Assert;

import org.junit.Test;

import com.netflix.astyanax.contrib.valve.TimeWindowValve.RequestStatus;

public class TimeWindowValveTest {

    @Test
    public void testSingleThread1SecWindow() throws Exception {
        
        TimeWindowValve window = new TimeWindowValve(1000L, System.currentTimeMillis(), 1000);
        testSingleThread(window, 100000, 1000);
    }
    
    @Test
    public void testSingleThread100MsWindow() throws Exception {
        
        TimeWindowValve window = new TimeWindowValve(1000L, System.currentTimeMillis(), 100);
        testSingleThread(window, 100000, 1000);
    }
    
    private void testSingleThread(final TimeWindowValve window, int numRequests, int expectedSuccesses) {
        
        Map<RequestStatus, Long> status = new HashMap<RequestStatus, Long>();
        
        for (int i=0; i<numRequests; i++) {
            RequestStatus ret = window.decrementAndCheckQuota();
            
            Long count = status.get(ret);
            if (count == null) {
                status.put(ret, 1L);
            } else {
                status.put(ret, ++count);
            }
        }
        
        Assert.assertTrue(expectedSuccesses == status.get(RequestStatus.Permitted));
    }
    
    @Test
    public void testMultipleThreads1SecWindow() throws Exception {
        
        final TimeWindowValve window = new TimeWindowValve(100000L, System.currentTimeMillis(), 1000);
        testMultipleThreads(window, 100000L, 300);
    }
    
    @Test
    public void testMultipleThreads100MsWindow() throws Exception {
        
        final TimeWindowValve window = new TimeWindowValve(10000L, System.currentTimeMillis(), 500);
        testMultipleThreads(window, 10000L, 300);
    }

    private void testMultipleThreads(final TimeWindowValve window, long expectedSuccesses, int runTestMillis) throws Exception {
        
        final MultiThreadTestControl testControl = new MultiThreadTestControl();
        
        // track success rate of rps that is allowed to pass through
        final AtomicLong successCount = new AtomicLong(0L);
        
        testControl.runTest(new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                RequestStatus status = window.decrementAndCheckQuota();
                if (status == RequestStatus.Permitted) {
                    successCount.incrementAndGet();
                }
                return null;
            }
        });
        
        Thread.sleep(runTestMillis);

        testControl.stopTest();
        
        long delta = Math.abs(expectedSuccesses-successCount.get());
        int percentageDiff = (int) (delta*100/expectedSuccesses);
        Assert.assertTrue("Success: " + successCount.get() + ", expected: " + expectedSuccesses + ", percentageDiff: " + percentageDiff, percentageDiff < 10);
    }
    
    @Test
    public void testPastWindow() throws Exception {
        
        final TimeWindowValve window = new TimeWindowValve(100000L, System.currentTimeMillis(), 100);
        
        for (int i=0; i<1000; i++) {
            RequestStatus status = window.decrementAndCheckQuota();
            Assert.assertEquals(RequestStatus.Permitted, status);
        }

        Thread.sleep(150);
        
        for (int i=0; i<1000; i++) {
            RequestStatus status = window.decrementAndCheckQuota();
            Assert.assertEquals(RequestStatus.PastWindow, status);
        }
    }
}
