package com.netflix.astyanax.util;

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;

public class BarrierCallableDecorator<T> implements Callable<T> {
    public CyclicBarrier barrier;
    public Callable<T> callable;
    
    public BarrierCallableDecorator(CyclicBarrier barrier, Callable<T> callable) {
        this.barrier = barrier;
        this.callable = callable;
    }
    
    @Override
    public T call() throws Exception {
        barrier.await();
        return callable.call();
    }

}
