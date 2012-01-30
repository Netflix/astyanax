package com.netflix.astyanax.connectionpool.impl;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.cliffc.high_scale_lib.NonBlockingHashSet;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy;

public class SmaLatencyScoreStrategyImpl implements LatencyScoreStrategy {

	private final ConnectionPoolConfiguration config;
	private final ScheduledExecutorService executor; 
	private final Set<Instance> instances;
	private final String name;
	
	public SmaLatencyScoreStrategyImpl(ConnectionPoolConfiguration config) {
		this(config, "SMA");
	}
	
	public SmaLatencyScoreStrategyImpl(ConnectionPoolConfiguration config, String name) {
		this.config = config;
		this.executor = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).build());	// TODO: is nThread==1 good?
		this.instances = new NonBlockingHashSet<Instance>();
		this.name = name;
	}
	
	protected Instance internalCreateInstance() {
		return new SmaLatencyScoreStrategyInstanceImpl(getConfig());
	}
	
	public final Instance createInstance() {
		Instance instance = internalCreateInstance();
		this.instances.add(instance);
		return instance;
	}

	@Override
	public void start(final Listener listener) {
		executor.schedule(new Runnable() {
			@Override
			public void run() {
				Thread.currentThread().setName(getName() + "_ScoreUpdate");
				long now = System.nanoTime();
				update(now);
				listener.onUpdate();
				executor.schedule(this, config.getLatencyAwareUpdateInterval(), TimeUnit.MILLISECONDS);
			}
		}, new Random().nextInt(config.getLatencyAwareUpdateInterval()), 
		   TimeUnit.MILLISECONDS);
	
		executor.schedule(new Runnable() {
			@Override
			public void run() {
				Thread.currentThread().setName(getName() + "_ScoreReset");
				reset();
				listener.onReset();
				executor.schedule(this, config.getLatencyAwareResetInterval(), TimeUnit.MILLISECONDS);
			}
		}, new Random().nextInt(config.getLatencyAwareResetInterval()), 
		   TimeUnit.MILLISECONDS);		}

	@Override
	public void shutdown() {
		executor.shutdown();
	}

	@Override
	public void removeInstance(Instance instance) {
		instances.remove(instance);
	}

	/**
	 * Comparator used to sort hosts by score
	 */
	private Comparator<HostConnectionPool<?>> scoreComparator = new Comparator<HostConnectionPool<?>>() {
		@Override
		public int compare(HostConnectionPool<?> p1, HostConnectionPool<?> p2) {
			double score1 = p1.getScore();
			double score2 = p2.getScore();
			if (score1 < score2) {
				return -1;
			}
			else if (score1 > score2) {
				return 1;
			}
			return 0;
		}
	};
	
	@Override
	public <CL> List<HostConnectionPool<CL>> sortAndfilterPartition(List<HostConnectionPool<CL>> srcPools, AtomicBoolean prioritized) {
		// Sort the candidate hosts by order of their score (low is good).  Then remove any host that
		// is more than badness threshold worse than than best host
		List<HostConnectionPool<CL>> pools = Lists.newArrayList(srcPools);
		Collections.sort(pools, scoreComparator);
		
		boolean hasBadHost = false;
		HostConnectionPool<?> firstPool = null;
		Iterator<HostConnectionPool<CL>> iter = pools.iterator();
		double firstScore = 0; 
		while (iter.hasNext()) {
			HostConnectionPool<CL> pool = iter.next();
			// The first pool is used as the base for comparing.  
			if (firstPool == null) {
				if (pool.getScore() > 0) {
					firstPool = pool;
					firstScore = pool.getScore();
				}
			}
			// Filter out pools with bad score
			else {
				double score = pool.getScore();
				if ((score / firstScore - 1) > config.getLatencyAwareBadnessThreshold()) {
					hasBadHost = true;
				}
			}
		}
		
		prioritized.set(!hasBadHost);
		if (!hasBadHost) {
			Collections.shuffle(pools);
		} 
		
		return pools;
	}

	public String getName() {
		return this.name;
	}
	
	public ConnectionPoolConfiguration getConfig() {
		return config;
	}
	
	public String toString() {
		return new StringBuilder().append(getName() + "[")
			.append("win=").append(config.getLatencyAwareWindowSize())
			.append(",rst=").append(config.getLatencyAwareResetInterval())
			.append(",upd=").append(config.getLatencyAwareUpdateInterval())
			.append(",trh=").append(config.getLatencyAwareBadnessThreshold())
			.append(",sen=").append(config.getLatencyAwareSentinelCompare())
			.append("]")
			.toString();
	}

	@Override
	public void update(long now) {
		for (Instance inst : instances) {
			inst.update(now);
		}
	}

	@Override
	public void reset() {
		for (Instance inst : instances) {
			inst.reset();
		}
	}	
}
