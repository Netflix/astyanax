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

import java.util.concurrent.LinkedBlockingDeque;

import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.RateLimiter;

public class SimpleRateLimiterImpl implements RateLimiter {
    private final LinkedBlockingDeque<Long> queue = new LinkedBlockingDeque<Long>();
    private final ConnectionPoolConfiguration config;

    public SimpleRateLimiterImpl(ConnectionPoolConfiguration config) {
        this.config = config;
    }

    @Override
    public boolean check() {
        return check(System.currentTimeMillis());
    }

    @Override
    public boolean check(long currentTimeMillis) {
        int maxCount = config.getConnectionLimiterMaxPendingCount();
        if (maxCount == 0)
            return true;

        // Haven't reached the count limit yet
        if (queue.size() < maxCount) {
            queue.addFirst(currentTimeMillis);
            return true;
        }
        else {
            long last = queue.getLast();
            if (currentTimeMillis - last < config.getConnectionLimiterWindowSize()) {
                return false;
            }
            queue.addFirst(currentTimeMillis);
            queue.removeLast();
            return true;
        }
    }

}
