package com.netflix.astyanax.shallows;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy;

public class EmptyLatencyScoreStrategyImpl implements LatencyScoreStrategy {

    public static EmptyLatencyScoreStrategyImpl instance = new EmptyLatencyScoreStrategyImpl();

    public static EmptyLatencyScoreStrategyImpl get() {
        return instance;
    }

    @Override
    public Instance createInstance() {
        return new Instance() {

            @Override
            public void addSample(long sample) {
            }

            @Override
            public double getScore() {
                return 0;
            }

            @Override
            public void reset() {
            }

            @Override
            public void update() {
            }
        };
    }

    @Override
    public void removeInstance(Instance instance) {
    }

    @Override
    public void start(Listener listener) {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void update() {
    }

    @Override
    public void reset() {
    }

    @Override
    public <CL> List<HostConnectionPool<CL>> sortAndfilterPartition(List<HostConnectionPool<CL>> pools,
            AtomicBoolean prioritized) {
        prioritized.set(false);
        return pools;
    }

    public String toString() {
        return "EmptyLatencyScoreStrategy[]";
    }

    @Override
    public int getUpdateInterval() {
        return 0;
    }

    @Override
    public int getResetInterval() {
        return 0;
    }

    @Override
    public double getScoreThreshold() {
        return 10.0;
    }

    @Override
    public int getBlockedThreshold() {
        return 100;
    }

    @Override
    public double getKeepRatio() {
        return 1.0;
    }
}
