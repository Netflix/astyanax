package com.netflix.astyanax.connectionpool.impl;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy;

public class SmaLatencyScoreStrategyInstanceImpl implements LatencyScoreStrategy.Instance {
	private final LinkedBlockingQueue<Long> latencies = new LinkedBlockingQueue<Long>();
	private final ConnectionPoolConfiguration config;
	private volatile Double cachedScore = 0.0d;
	private AtomicLong lastSampleTime = new AtomicLong(0);
	
	public SmaLatencyScoreStrategyInstanceImpl(ConnectionPoolConfiguration config) {
		this.config = config;
	}
	
	@Override
	public void addSample(long sample, long now) {
		lastSampleTime.set(now);
		latencies.add(sample);
		if (latencies.size() > config.getLatencyAwareWindowSize()) {
			latencies.remove();
		}
	}
	
	@Override
	public double getScore() {
		return cachedScore;
	}

	@Override
	public void reset() {
		latencies.add(0l);
	}
	
	@Override
	public void update(long now) {
		cachedScore = calculateCurrentScore(now);
	}
	
	@Override
	public long getLastSampleTime() {
		return lastSampleTime.get();
	}
	
	protected double calculateCurrentScore(long now) {
		return getMean();
	}

	@Override
	public double getMean() {
	    long total = 0;
	    int count = 0;
	    for (long d : latencies) {
	      total += d;
	      count++;
	    }
	    if (count == 0 || count < config.getLatencyAwareWindowSize()/2)
	    	return 0;
	    return total / count;
	}
}
