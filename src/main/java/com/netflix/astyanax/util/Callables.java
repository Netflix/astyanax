package com.netflix.astyanax.util;

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;

public class Callables {
    /**
     * Create a callable that waits on a barrier before starting execution
     * @param barrier
     * @param callable
     * @return
     */
    public static <T> Callable<T> decorateWithBarrier(CyclicBarrier barrier, Callable<T> callable) {
        return new BarrierCallableDecorator<T>(barrier, callable);
    }
}
