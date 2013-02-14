package com.netflix.astyanax.test;

import com.google.common.base.Supplier;

public class IncreasingRateSupplier implements Supplier<Integer>{
    private int rate;
    private final long delta;
    
    public IncreasingRateSupplier(int initialRate, int delta) {
        this.rate = initialRate;
        this.delta = delta;
    }
    
    public Integer get() {
        rate += this.delta;
        return rate;
    }
}
