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

import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.RetryBackoffStrategy;

/**
 * Impl for {@link RetryBackoffStrategy} that is used to reconnect a {@link HostConnectionPool} when a host is marked as down. 
 * 
 * @author elandau 
 * @see {@link SimpleHostConnectionPool#markAsDown(com.netflix.astyanax.connectionpool.exceptions.ConnectionException)} for details on how this class
 * could be referenced (if needed)
 */
public class FixedRetryBackoffStrategy implements RetryBackoffStrategy {
    private final int interval;
    private final int suspendTime;

    public FixedRetryBackoffStrategy(int interval, int suspendTime) {
        this.interval = interval;
        this.suspendTime = suspendTime;
    }

    @Override
    public Instance createInstance() {
        return new RetryBackoffStrategy.Instance() {
            private boolean isSuspended = false;
            private int attemptCount = 0;

            @Override
            public long getNextDelay() {
                if (isSuspended) {
                    isSuspended = false;
                    return suspendTime;
                }
                return interval;
            }

            @Override
            public int getAttemptCount() {
                return attemptCount;
            }

            @Override
            public void begin() {
                attemptCount = 0;
            }

            @Override
            public void success() {
            }

            @Override
            public void suspend() {
            }
        };
    }
}
