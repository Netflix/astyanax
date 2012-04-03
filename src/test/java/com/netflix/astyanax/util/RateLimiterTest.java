package com.netflix.astyanax.util;

import junit.framework.Assert;

import org.junit.Test;

import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.SimpleRateLimiterImpl;

public class RateLimiterTest {
    @Test
    public void testRateLimiter() {
        ConnectionPoolConfigurationImpl config = new ConnectionPoolConfigurationImpl(
                "cluster_keyspace");
        config.setConnectionLimiterMaxPendingCount(10);
        config.setConnectionLimiterWindowSize(1000);

        SimpleRateLimiterImpl limit = new SimpleRateLimiterImpl(config);

        int time = 0;
        boolean result;
        for (int i = 0; i < 10; i++) {
            time += 10;
            result = limit.check(time);
            Assert.assertTrue(result);
        }
        result = limit.check(time + 10);
        Assert.assertFalse(result);

        result = limit.check(time + 1000);
        Assert.assertTrue(result);
    }

    @Test
    public void testRateLimiter2() {
        ConnectionPoolConfigurationImpl config = new ConnectionPoolConfigurationImpl(
                "cluster_keyspace");
        config.setConnectionLimiterMaxPendingCount(10);
        config.setConnectionLimiterWindowSize(1000);

        SimpleRateLimiterImpl limit = new SimpleRateLimiterImpl(config);

        int time = 0;
        int interval = 100;
        for (int i = 0; i < 10; i++) {
            time += interval;
            boolean result = limit.check(time);
            Assert.assertTrue(result);
        }
        boolean result = limit.check(time + interval);
        Assert.assertTrue(result);
    }
}
