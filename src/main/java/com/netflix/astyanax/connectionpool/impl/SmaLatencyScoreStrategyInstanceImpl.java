package com.netflix.astyanax.connectionpool.impl;

import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;

import com.netflix.astyanax.connectionpool.LatencyScoreStrategy;

public class SmaLatencyScoreStrategyInstanceImpl implements
        LatencyScoreStrategy.Instance {
    private final LinkedBlockingQueue<Long> latencies;
    private volatile Double cachedScore = 0.0d;

    public SmaLatencyScoreStrategyInstanceImpl(
            SmaLatencyScoreStrategyImpl strategy) {
        this.latencies = new LinkedBlockingQueue<Long>(strategy.getWindowSize());
    }

    @Override
    public void addSample(long sample) {
        if (!latencies.offer(sample)) {
            try {
                latencies.remove();
            } catch (NoSuchElementException e) {
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

    @Override
    public double getMean() {
        long sum = 0;
        int count = 0;
        for (long d : latencies) {
            sum += d;
            count++;
        }
        return (count > 0) ? sum / count : 0.0;
    }
}
