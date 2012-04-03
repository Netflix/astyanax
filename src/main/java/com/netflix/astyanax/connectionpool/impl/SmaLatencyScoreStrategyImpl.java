package com.netflix.astyanax.connectionpool.impl;

import java.util.Collections;
import java.util.Comparator;
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
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy;

public class SmaLatencyScoreStrategyImpl implements LatencyScoreStrategy {

    private final ScheduledExecutorService executor;
    private final Set<Instance> instances;
    private final int updateInterval;
    private final int resetInterval;
    private final int windowSize;
    private final double badnessThreshold;

    public SmaLatencyScoreStrategyImpl(int updateInterval, int resetInterval,
            int windowSize, double badnessThreshold) {
        this.updateInterval = updateInterval;
        this.resetInterval = resetInterval;
        this.badnessThreshold = badnessThreshold;
        this.windowSize = windowSize;

        this.executor = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setDaemon(true).build());
        this.instances = new NonBlockingHashSet<Instance>();
    }

    protected Instance internalCreateInstance() {
        return new SmaLatencyScoreStrategyInstanceImpl(this);
    }

    public final Instance createInstance() {
        Instance instance = internalCreateInstance();
        this.instances.add(instance);
        return instance;
    }

    public int getUpdateInterval() {
        return updateInterval;
    }

    public int getResetInterval() {
        return resetInterval;
    }

    public double getBadnessThreshold() {
        return badnessThreshold;
    }

    public int getWindowSize() {
        return windowSize;
    }

    @Override
    public void start(final Listener listener) {
        executor.schedule(new Runnable() {
            @Override
            public void run() {
                Thread.currentThread().setName(getName() + "_ScoreUpdate");
                update();
                listener.onUpdate();
                executor.schedule(this, getUpdateInterval(),
                        TimeUnit.MILLISECONDS);
            }
        }, new Random().nextInt(getUpdateInterval()), TimeUnit.MILLISECONDS);

        executor.schedule(new Runnable() {
            @Override
            public void run() {
                Thread.currentThread().setName(getName() + "_ScoreReset");
                reset();
                listener.onReset();
                executor.schedule(this, getResetInterval(),
                        TimeUnit.MILLISECONDS);
            }
        }, new Random().nextInt(getResetInterval()), TimeUnit.MILLISECONDS);
    }

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
            } else if (score1 > score2) {
                return 1;
            }
            return 0;
        }
    };

    @Override
    public <CL> List<HostConnectionPool<CL>> sortAndfilterPartition(
            List<HostConnectionPool<CL>> srcPools, AtomicBoolean prioritized) {
        List<HostConnectionPool<CL>> pools = Lists.newArrayList(srcPools);
        Collections.sort(pools, scoreComparator);
        prioritized.set(true);
        return pools;
    }

    public String getName() {
        return "SMA";
    }

    public String toString() {
        return new StringBuilder().append(getName() + "[").append("win=")
                .append(getWindowSize()).append(",rst=")
                .append(getResetInterval()).append(",upd=")
                .append(getUpdateInterval()).append(",trh=")
                .append(getBadnessThreshold()).append("]").toString();
    }

    @Override
    public void update() {
        for (Instance inst : instances) {
            inst.update();
        }
    }

    @Override
    public void reset() {
        for (Instance inst : instances) {
            inst.reset();
        }
    }
}
