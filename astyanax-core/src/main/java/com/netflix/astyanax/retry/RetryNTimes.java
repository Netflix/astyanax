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

public class RetryNTimes implements RetryPolicy {
    private final int maxAttemptCount;
    private int attempts;

    public RetryNTimes(int maxAttemptCount) {
        this.maxAttemptCount = maxAttemptCount;
        this.attempts = 0;
    }

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

    @Override
    public boolean allowRetry() {
        if (maxAttemptCount == -1 || attempts < maxAttemptCount) {
            attempts++;
            return true;
        }
        return false;
    }

    @Override
    public int getAttemptCount() {
        return attempts;
    }

    public int getMaxAttemptCount() {
        return maxAttemptCount;
    }

    @Override
    public RetryPolicy duplicate() {
        return new RetryNTimes(maxAttemptCount);
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
