package com.netflix.astyanax.contrib.valve;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class MultiThreadTestControl {

    // threads and thread pool
    final int numThreads;
    ExecutorService threadPool;

    // multi-thread test control
    // barrier tells all threads to wait.
    final CyclicBarrier barrier;

    // atomic boolean tells them to stop running
    final AtomicBoolean stop = new AtomicBoolean(false);
    
    // latch tells main test that threads have stopped running and hence successCount is not moving fwd
    final CountDownLatch latch;

    public MultiThreadTestControl() {
        this(8);
    }
    
    public MultiThreadTestControl(int nThreads) {
        numThreads = nThreads;
        barrier = new CyclicBarrier(numThreads);
        latch = new CountDownLatch(numThreads);
    }

    public void runTest(final Callable<Void> perThreadIterationCall) {
        
        threadPool = Executors.newFixedThreadPool(numThreads);
        
        for (int i=0; i<numThreads; i++) {
            
            threadPool.submit(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    
                    barrier.await();
                    
                    while (!stop.get()) {
                        perThreadIterationCall.call();
                    }
                    // stopping test thread
                    latch.countDown();
                    return null;
                }
            });
        }
     
    }
    
    public void stopTest() {
        
        stop.set(true);
        threadPool.shutdownNow();
        try {
            latch.await(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    
}
    


