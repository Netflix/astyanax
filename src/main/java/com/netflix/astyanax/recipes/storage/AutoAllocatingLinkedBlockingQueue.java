package com.netflix.astyanax.recipes.storage;

import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.base.Supplier;

@SuppressWarnings("serial")
public class AutoAllocatingLinkedBlockingQueue<T> extends LinkedBlockingQueue<T> {

    public AutoAllocatingLinkedBlockingQueue(int concurrencyLevel) {
        super(concurrencyLevel);
    }

    public T poll(Supplier<T> supplier) {
        T bb = super.poll();
        if (bb == null) {
            return supplier.get();
        }
        return bb;
    }
}
