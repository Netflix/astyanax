package com.netflix.astyanax.connectionpool.impl;

import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.Lists;

/**
 * Calculate latency as an exponential moving average.
 * 
 * @author elandau
 */
public class EmaLatencyScoreStrategyImpl extends AbstractLatencyScoreStrategyImpl {
    private final static String NAME = "EMA";
    
    private final int    N; // Number of samples
    private final double k; // cached value for calculation
    private final double one_minus_k; // cached value for calculation
    
    public EmaLatencyScoreStrategyImpl(int updateInterval, int resetInterval, int windowSize, int blockedThreshold, double keepRatio, double scoreThreshold) {
        super(NAME, updateInterval, resetInterval, blockedThreshold, keepRatio, scoreThreshold);
        
        this.N = windowSize;
        this.k = (double)2 / (double)(this.N + 1);
        this.one_minus_k = 1 - this.k;
    }
    
    public EmaLatencyScoreStrategyImpl(int updateInterval, int resetInterval, int windowSize) {
        super(NAME, updateInterval, resetInterval);
        
        this.N = windowSize;
        this.k = (double)2 / (double)(this.N + 1);
        this.one_minus_k = 1 - this.k;
    }
    
    public EmaLatencyScoreStrategyImpl(int windowSize) {
        super(NAME);
        
        this.N = windowSize;
        this.k = (double)2 / (double)(this.N + 1);
        this.one_minus_k = 1 - this.k;
    }
    
    @Override
    public final Instance newInstance() {
        return new Instance() {
            private final LinkedBlockingQueue<Long> latencies = new LinkedBlockingQueue<Long>(N);
            private volatile double cachedScore = 0.0d;
    
            @Override
            public void addSample(long sample) {
                if (!latencies.offer(sample)) {
                    try {
                        latencies.remove();
                    }
                    catch (NoSuchElementException e) {
                    }
                    latencies.offer(sample);
                }
            }
    
            @Override
            public double getScore() {
                return cachedScore;
            }
    
            @Override
            public void reset() {
                cachedScore = 0.0;
                latencies.clear();
            }
    
            /**
             * Drain all the samples and update the cached score
             */
            @Override
            public void update() {
                Double ema = cachedScore;
                ArrayList<Long> samples = Lists.newArrayList();
                latencies.drainTo(samples);
                if (samples.size() == 0) {
                    samples.add(0L);
                }                    
                
                if (ema == 0.0) {
                    ema = (double)samples.remove(0);
                }
                for (Long sample : samples) {
                    ema = sample * k + ema * one_minus_k;
                }
                cachedScore = ema;
            }
        };
    }
}
