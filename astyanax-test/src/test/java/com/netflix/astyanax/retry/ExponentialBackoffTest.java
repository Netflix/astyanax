package com.netflix.astyanax.retry;

import java.lang.reflect.Field;

import static org.junit.Assert.assertTrue;
import org.junit.Test;

public final class ExponentialBackoffTest {


    @Test
    public void testSleepTimeNeverNegative() throws NoSuchFieldException, IllegalAccessException {
        ExponentialBackoff backoff = new ExponentialBackoff(500, -1);

        for(int i = 22; i < 1000; i++) {
            setAttemptCount(backoff, i);
            assertTrue("Backoff at retry " + i + " was not positive", backoff.getSleepTimeMs() >= 0);
        }
    }

    public static void setAttemptCount(SleepingRetryPolicy backoff, int attempt)
            throws NoSuchFieldException, IllegalAccessException {
        Field attempts = SleepingRetryPolicy.class.getDeclaredField("attempts");
        attempts.setAccessible(true);
        attempts.setInt(backoff, attempt);
    }
}