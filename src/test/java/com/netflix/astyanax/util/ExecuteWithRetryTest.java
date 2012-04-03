package com.netflix.astyanax.util;

import org.junit.Test;

import com.netflix.astyanax.Execution;

public class ExecuteWithRetryTest {

    public static class ExecuteWithRetry<T> {
        private final Execution<T> execution;

        public ExecuteWithRetry(Execution<T> execution) {
            this.execution = execution;
        }

    };

    @Test
    public void testRetry() {
    }
}
