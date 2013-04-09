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
 * Base sleeping retry policy with optional count limit.  The sleep time
 * is delegated to the subclass.
 * 
 * @author elandau
 * 
 */
public abstract class SleepingRetryPolicy implements RetryPolicy {
    private final int maxAttempts;
    private int attempts;

    public SleepingRetryPolicy(int max) {
        this.maxAttempts = max;
        this.attempts = 0;
    }

    public boolean allowRetry() {
        if (maxAttempts == -1 || attempts < maxAttempts) {
            try {
                Thread.sleep(getSleepTimeMs());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            attempts++;
            return true;
        }
        return false;
    }

    public abstract long getSleepTimeMs();

    @Override
    public void begin() {
        this.attempts = 0;
    }

    @Override
    public void success() {
    }

    @Override
    public void failure(Exception e) {
    }

    public int getAttemptCount() {
        return attempts;
    }

    @Deprecated
    public int getMax() {
        return getMaxAttemptCount();
    }
    
    public int getMaxAttemptCount() {
        return maxAttempts;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
