package com.netflix.astyanax.connectionpool.impl;

import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;

public class SmaLatencyScoreStrategyImpl extends AbstractLatencyScoreStrategyImpl {
    private static final String NAME = "SMA";
    
    private final int    windowSize;

    public SmaLatencyScoreStrategyImpl(int updateInterval, int resetInterval, int windowSize, int blockedThreshold, double keepRatio, double scoreThreshold) {
        super(NAME, updateInterval, resetInterval, blockedThreshold, keepRatio, scoreThreshold);
        this.windowSize     = windowSize;
    }
    
    public SmaLatencyScoreStrategyImpl(int updateInterval, int resetInterval, int windowSize, double badnessThreshold) {
        this(updateInterval, resetInterval, windowSize, DEFAULT_BLOCKED_THREAD_THRESHOLD, DEFAULT_KEEP_RATIO, badnessThreshold);
    }

    public SmaLatencyScoreStrategyImpl() {
        super(NAME);
        this.windowSize = 20;
    }

    public final Instance newInstance() {
        return new Instance() {
            private final LinkedBlockingQueue<Long> latencies = new LinkedBlockingQueue<Long>(windowSize);
            private volatile Double cachedScore = 0.0d;
    
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
                latencies.clear();
            }
    
            @Override
            public void update() {
                cachedScore = getMean();
            }
    
            private double getMean() {
                long sum = 0;
                int count = 0;
                for (long d : latencies) {
                    sum += d;
                    count++;
                }
                return (count > 0) ? sum / count : 0.0;
            }
        };
    }
}
