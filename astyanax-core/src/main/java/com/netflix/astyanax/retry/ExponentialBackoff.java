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
package com.netflix.astyanax.retry;

import java.util.Random;

import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * Unbounded exponential backoff will sleep a random number of intervals within an
 * exponentially increasing number of intervals.  
 * 
 * @author elandau
 *
 */
public class ExponentialBackoff extends SleepingRetryPolicy {
    private final int MAX_SHIFT = 30;
    
    private final Random random = new Random();
    private final int baseSleepTimeMs;

    public ExponentialBackoff(int baseSleepTimeMs, int maxAttempts) {
        super(maxAttempts);
        this.baseSleepTimeMs = baseSleepTimeMs;
    }

    @Override
    public long getSleepTimeMs() {
        int attempt = this.getAttemptCount() + 1;
        if (attempt > MAX_SHIFT) {
            attempt = MAX_SHIFT;
        }
        return baseSleepTimeMs * Math.max(1, random.nextInt(1 << attempt));
    }
    
    public int getBaseSleepTimeMs() {
        return baseSleepTimeMs;
    }
    
    @Override
    public RetryPolicy duplicate() {
        return new ExponentialBackoff(baseSleepTimeMs, getMaxAttemptCount());
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
