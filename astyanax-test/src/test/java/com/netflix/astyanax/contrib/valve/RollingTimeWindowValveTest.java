package com.netflix.astyanax.contrib.valve;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

public class RollingTimeWindowValveTest {

 
    public void testSingleThreadSingleBucket() throws Exception {
     
        final RollingTimeWindowValve valve = new RollingTimeWindowValve(10000, 1);
        
        Long counter = new Long(0L);
        
        for (int i=0; i<100000; i++) {
            boolean success = valve.decrementAndCheckQuota();
            if (success) {
                counter++;
            }
        }
        
        Assert.assertTrue("Success: " + counter, counter == 10000);
    }
    

    public void testSingleThreadMultipleBucketsSingleSecond() throws Exception {

        final RollingTimeWindowValve valve = new RollingTimeWindowValve(10000, 10);
        runMultiThreadTest(valve, 1, 750, 8000);
    }

 
    public void testSingleThreadMultipleBucketsMultipleSeconds() throws Exception {

        final RollingTimeWindowValve valve = new RollingTimeWindowValve(10000, 10);
        runMultiThreadTest(valve, 1, 1750, 18000);
    }


    public void testMultipleThreadsSingleBucketSingleSecond() throws Exception {
     
        final RollingTimeWindowValve valve = new RollingTimeWindowValve(10000, 1);
        runMultiThreadTest(valve, 8, 800, 10000);
    }
    

    public void testMultipleThreadsSingleBucketMultipleSeconds() throws Exception {
     
        final RollingTimeWindowValve valve = new RollingTimeWindowValve(10000, 1);
        runMultiThreadTest(valve, 8, 1700, 20000);
    }


    public void testMultipleThreadsMultipleBucketsMultipleSeconds() throws Exception {
     
        final RollingTimeWindowValve valve = new RollingTimeWindowValve(10000, 10);
        runMultiThreadTest(valve, 8, 1750, 18000);
    }

    private void runMultiThreadTest(final RollingTimeWindowValve valve, int numThreads, int sleepMillis, long expectedSuccesses) throws Exception {
        
        final AtomicLong successCount = new AtomicLong(0L);
        final MultiThreadTestControl testControl = new MultiThreadTestControl(numThreads);
        
        testControl.runTest(new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                boolean success = valve.decrementAndCheckQuota();
                if (success) {
                    successCount.incrementAndGet();
                }
                return null;
            }
        });

        Thread.sleep(sleepMillis);
        
        testControl.stopTest();
        
        long delta = Math.abs(expectedSuccesses-successCount.get());
        int percentageDiff = (int) (delta*100/expectedSuccesses);
        
        Assert.assertTrue("Success: " + successCount.get() + ", expected: " + expectedSuccesses + ", percentageDiff: " + percentageDiff, percentageDiff < 10);
        //System.out.println("Success: " + successCount.get() + ", expected: " + expectedSuccesses + ", percentageDiff: " + percentageDiff);
    }
    
 
    public void testChangeInRate() throws Exception {
        
        final RollingTimeWindowValve valve = new RollingTimeWindowValve(10000, 10);
        
        final AtomicReference<PauseTest> pause = new AtomicReference<PauseTest>(new PauseTest(8)); 
        
        final AtomicLong successCount = new AtomicLong(0L);
        final MultiThreadTestControl testControl = new MultiThreadTestControl(8);
        
        testControl.runTest(new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                
                if (pause.get().shouldPause()) {
                    pause.get().waitOnResume();
                }
                
                boolean success = valve.decrementAndCheckQuota();
                if (success) {
                    successCount.incrementAndGet();
                }
                return null;
            }
        });

        Thread.sleep(1450);
        
        pause.get().pauseTest();
        valve.setRatePerSecond(20000L);
        pause.get().resumeTest();
        
        Thread.sleep(970);
        
        pause.set(new PauseTest(8));
        pause.get().pauseTest();
        valve.setRatePerSecond(5000L);
        pause.get().resumeTest();
        
        Thread.sleep(1000);

        testControl.stopTest();
        
        long expectedSuccesses = 40000;  // 10000 for the 1st 1000 ms. and then 20000 for the next 1000 ms and 5000 for the last 1000 ms
        long delta = Math.abs(expectedSuccesses-successCount.get());
        int percentageDiff = (int) (delta*100/expectedSuccesses);
        
        //Assert.assertTrue("Success: " + successCount.get() + ", expected: " + expectedSuccesses + ", percentageDiff: " + percentageDiff, percentageDiff < 10);
        //System.out.println("Success: " + successCount.get() + ", expected: " + expectedSuccesses + ", percentageDiff: " + percentageDiff);
    }
    
    private class PauseTest {
        
        private final CountDownLatch waitLatch = new CountDownLatch(1);
        
        private final AtomicBoolean pauseEnabled = new AtomicBoolean(false);
        
        private PauseTest(int numWorkersToPause) {
        }
        
        private void pauseTest() {
            pauseEnabled.set(true);
        }
        
        private void resumeTest() {
            pauseEnabled.set(false);
            waitLatch.countDown();
        }
        
        private boolean shouldPause() {
            return pauseEnabled.get();
        }
        
        private void waitOnResume() {
            try {
                waitLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
}
