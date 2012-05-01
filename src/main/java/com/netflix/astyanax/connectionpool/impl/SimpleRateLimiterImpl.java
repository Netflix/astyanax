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
