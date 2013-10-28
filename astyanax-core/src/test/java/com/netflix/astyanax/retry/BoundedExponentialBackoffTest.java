package com.netflix.astyanax.retry;

import static com.netflix.astyanax.retry.ExponentialBackoffTest.setAttemptCount;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public final class BoundedExponentialBackoffTest {

    @Test
    public void testSleepTimeNeverNegative() throws NoSuchFieldException, IllegalAccessException {
        BoundedExponentialBackoff backoff = new BoundedExponentialBackoff(500, 5000, -1);

        for(int i = 0; i < 1000; i++) {
            setAttemptCount(backoff, i);
            assertTrue("Backoff at retry " + i + " was not positive", backoff.getSleepTimeMs() >= 0);
        }
    }
}