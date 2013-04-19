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

import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * Bounded exponential backoff that will wait for no more than a provided max amount of time.
 * 
 * The following examples show the maximum wait time for each attempt
 *  ExponentalBackoff(250, 10) 
 *      250 500 1000 2000 4000 8000 16000 32000 64000 128000
 *  
 *  BoundedExponentialBackoff(250, 5000, 10) 
 *      250 500 1000 2000 4000 5000 5000 5000 5000 5000
 * 
 * @author elandau
 *
 */
public class BoundedExponentialBackoff extends ExponentialBackoff {

    private final int maxSleepTimeMs;

    public BoundedExponentialBackoff(int baseSleepTimeMs, int maxSleepTimeMs, int max) {
        super(baseSleepTimeMs, max);
        this.maxSleepTimeMs = maxSleepTimeMs;
    }

    public long getSleepTimeMs() {
        return Math.min(maxSleepTimeMs, super.getSleepTimeMs());
    }

    @Override
    public RetryPolicy duplicate() {
        return new BoundedExponentialBackoff(getBaseSleepTimeMs(), maxSleepTimeMs, getMaxAttemptCount());
    }

    public int getMaxSleepTimeMs() {
        return maxSleepTimeMs;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
