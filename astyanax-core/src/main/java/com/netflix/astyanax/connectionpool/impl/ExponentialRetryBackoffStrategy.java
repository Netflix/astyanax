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
package com.netflix.astyanax.connectionpool.impl;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.RetryBackoffStrategy;

/**
 * Impl for {@link RetryBackoffStrategy} that is used to reconnect a {@link HostConnectionPool} when a host is marked as down. 
 * 
 * @author elandau 
 * @see {@link SimpleHostConnectionPool#markAsDown(com.netflix.astyanax.connectionpool.exceptions.ConnectionException)} for details on how this class is referenced
 */
public class ExponentialRetryBackoffStrategy implements RetryBackoffStrategy {
    private final ConnectionPoolConfiguration config;

    /**
     * @param config
     */
    public ExponentialRetryBackoffStrategy(ConnectionPoolConfiguration config) {
        this.config = config;
    }

    /**
     * @return String 
     */
    public String toString() {
        return new StringBuilder().append("ExpRetry[").append("max=").append(config.getRetryMaxDelaySlice())
                .append(",slot=").append(config.getRetryDelaySlice()).append(",suspend=")
                .append(config.getRetrySuspendWindow()).append("]").toString();
    }

    /**
     * @return {@link Instance}
     */
    @Override
    public Instance createInstance() {
        return new RetryBackoffStrategy.Instance() {
            private int c = 1;
            private AtomicBoolean isSuspended = new AtomicBoolean(false);
            private int attemptCount = 0;
            private long lastReconnectTime = 0;

            @Override
            public long getNextDelay() {
                if (isSuspended.get()) {
                    isSuspended.set(false);
                    return config.getRetrySuspendWindow();
                }

                attemptCount++;
                if (attemptCount == 1) {
                    if (System.currentTimeMillis() - lastReconnectTime < config.getRetrySuspendWindow()) {
                        return config.getRetrySuspendWindow();
                    }
                }

                c *= 2;
                if (c > config.getRetryMaxDelaySlice())
                    c = config.getRetryMaxDelaySlice();

                return (new Random().nextInt(c) + 1) * config.getRetryDelaySlice();
            }

            @Override
            public int getAttemptCount() {
                return attemptCount;
            }

            public String toString() {
                return new StringBuilder().append("ExpRetry.Instance[").append(c).append(",").append(isSuspended)
                        .append(",").append(attemptCount).append("]").toString();
            }

            @Override
            public void begin() {
                this.attemptCount = 0;
                this.c = 1;
            }

            @Override
            public void success() {
                this.lastReconnectTime = System.currentTimeMillis();
            }

            @Override
            public void suspend() {
                this.isSuspended.set(true);
            }
        };
    }
}
