/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
