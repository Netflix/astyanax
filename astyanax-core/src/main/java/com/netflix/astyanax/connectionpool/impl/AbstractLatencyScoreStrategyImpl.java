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
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy;

/**
 * @author elandau
 */
public abstract class AbstractLatencyScoreStrategyImpl implements LatencyScoreStrategy {
    
    public static final int DEFAULT_UPDATE_INTERVAL          = 1000;
    public static final int DEFAULT_RESET_INTERVAL           = 0;
    public static final int DEFAULT_BLOCKED_THREAD_THRESHOLD = 10;
    public static final double DEFAULT_KEEP_RATIO            = 0.65;
    public static final double DEFAULT_SCORE_THRESHOLD       = 2.0;
    
    private final ScheduledExecutorService executor;
    private final Set<Instance>            instances;
    private final int                      updateInterval;
    private final int                      resetInterval;
    private final double                   scoreThreshold;
    private final int                      blockedThreshold;
    private final String                   name;
    private final double                   keepRatio;
    private boolean                  bOwnedExecutor = false;

    public AbstractLatencyScoreStrategyImpl(String name, int updateInterval, int resetInterval, int blockedThreshold, double keepRatio, double scoreThreshold, ScheduledExecutorService executor) {
        this.updateInterval   = updateInterval;
        this.resetInterval    = resetInterval;
        this.name             = name;
        this.scoreThreshold   = scoreThreshold;
        this.blockedThreshold = blockedThreshold;
        this.keepRatio        = keepRatio;

        this.executor  = executor;
        this.instances = new NonBlockingHashSet<Instance>();
    }
    
    /**
     * 
     * @param name
     * @param updateInterval    In milliseconds
     * @param resetInterval     In milliseconds
     */
    public AbstractLatencyScoreStrategyImpl(String name, int updateInterval, int resetInterval, int blockedThreshold, double keepRatio, double scoreThreshold) {
        this(name, updateInterval, resetInterval, blockedThreshold, keepRatio, scoreThreshold, Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).build()));
        bOwnedExecutor = true;
    }

    public AbstractLatencyScoreStrategyImpl(String name, int updateInterval, int resetInterval) {
        this(name, updateInterval, resetInterval, DEFAULT_BLOCKED_THREAD_THRESHOLD, DEFAULT_KEEP_RATIO, DEFAULT_SCORE_THRESHOLD);
    }
    
    public AbstractLatencyScoreStrategyImpl(String name) {
        this(name, DEFAULT_UPDATE_INTERVAL, DEFAULT_RESET_INTERVAL, DEFAULT_BLOCKED_THREAD_THRESHOLD, DEFAULT_KEEP_RATIO, DEFAULT_SCORE_THRESHOLD);
    }
    
    public final Instance createInstance() {
        Instance instance = newInstance();
        instances.add(instance);
        return instance;
    }
    
    /**
     * Template method for creating a new latency tracking instance for a host
     * @return
     */
    protected abstract Instance newInstance();

    @Override
    public void start(final Listener listener) {
        if (updateInterval > 0) {
            executor.schedule(new Runnable() {
                @Override
                public void run() {
                    Thread.currentThread().setName(name + "_ScoreUpdate");
                    update();
                    listener.onUpdate();
                    executor.schedule(this, getUpdateInterval(), TimeUnit.MILLISECONDS);
                }
            }, new Random().nextInt(getUpdateInterval()), TimeUnit.MILLISECONDS);
        }

        if (resetInterval > 0) {
            executor.schedule(new Runnable() {
                @Override
                public void run() {
                    Thread.currentThread().setName(name + "_ScoreReset");
                    reset();
                    listener.onReset();
                    executor.schedule(this, getResetInterval(), TimeUnit.MILLISECONDS);
                }
            }, new Random().nextInt(getResetInterval()), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void shutdown() {
        if (bOwnedExecutor)
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

    /**
     * Comparator used to sort hosts by number of buys + blocked operations
     */
    private Comparator<HostConnectionPool<?>> busyComparator = new Comparator<HostConnectionPool<?>>() {
        @Override
        public int compare(HostConnectionPool<?> p1, HostConnectionPool<?> p2) {
            return p1.getBusyConnectionCount() + p1.getBlockedThreadCount() - p2.getBusyConnectionCount() - p2.getBlockedThreadCount();
        }
    };

    @Override
    public <CL> List<HostConnectionPool<CL>> sortAndfilterPartition(List<HostConnectionPool<CL>> srcPools,
            AtomicBoolean prioritized) {
        List<HostConnectionPool<CL>> pools = Lists.newArrayList(srcPools);
        Collections.sort(pools, scoreComparator);
        prioritized.set(false);
        int poolSize = pools.size();
        int keep     = (int) Math.max(1, Math.ceil(poolSize * getKeepRatio()));

        // Step 1: Remove any host that is current reconnecting
        Iterator<HostConnectionPool<CL>> iter = pools.iterator();
        while (iter.hasNext()) {
            HostConnectionPool<CL> pool = iter.next();
            if (pool.isReconnecting()) {
//                System.out.println("**** Removing host (reconnecting) : " + pool.toString());
                iter.remove();
            }
        }
       
	//step 2  
        if (pools.size() > 0) {
            // Step 3: Filter out hosts that are too slow and keep at least the best keepRatio hosts
            int first = 0;
            for (; pools.get(0).getScore() == 0.0 && first < pools.size(); first++);
            
            if (first < pools.size()) {
                double scoreFirst = pools.get(first).getScore();
//                System.out.println("First : " + scoreFirst);
                if (scoreFirst > 0.0) {
                    for (int i = pools.size() - 1; i >= keep && i > first; i--) {
                        HostConnectionPool<CL> pool  = pools.get(i);
//                        System.out.println(i + " : " + pool.getScore() + " threshold:" + getScoreThreshold());
                        if ((pool.getScore() / scoreFirst) > getScoreThreshold()) {
//                            System.out.println("**** Removing host (score) : " + pool.toString());
                            pools.remove(i);
                        }
                        else {
                            break;
                        }
                    }
                }
            }
        }
        // Step 3: Filter out hosts that are too slow and keep at least the best keepRatio hosts
        if (pools.size() > keep) {
            Collections.sort(pools, busyComparator);
            HostConnectionPool<CL> poolFirst = pools.get(0);
            int firstBusy = poolFirst.getBusyConnectionCount() - poolFirst.getBlockedThreadCount();
            for (int i = pools.size() - 1; i >= keep; i--) {
                HostConnectionPool<CL> pool  = pools.get(i);
                int busy = pool.getBusyConnectionCount() + pool.getBlockedThreadCount();
                if ( (busy - firstBusy) > getBlockedThreshold()) {
//                    System.out.println("**** Removing host (blocked) : " + pool.toString());
                    pools.remove(i);
                }
            }
        }
        
        // Step 4: Shuffle the hosts 
        Collections.shuffle(pools);
        
        return pools;
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
    
    @Override
    public int getUpdateInterval() {
        return this.updateInterval;
    }
    
    @Override
    public int getResetInterval() {
        return this.resetInterval;
    }
    
    @Override
    public double getScoreThreshold() {
        return scoreThreshold;
    }

    @Override
    public int getBlockedThreshold() {
        return this.blockedThreshold;
    }
    
    @Override
    public double getKeepRatio() {
        return this.keepRatio;
    }

}
