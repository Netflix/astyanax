/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
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
