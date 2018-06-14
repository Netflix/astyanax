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
package com.netflix.astyanax.retry;

public class IndefiniteRetry implements RetryPolicy {

    private int counter = 1;
    
    @Override
    public void begin() {
        counter = 1;
    }

    @Override
    public void success() {
    }

    @Override
    public void failure(Exception e) {
        counter++;
    }

    @Override
    public boolean allowRetry() {
        return true;
    }

    @Override
    public int getAttemptCount() {
        return counter;
    }

    @Override
    public RetryPolicy duplicate() {
        return new IndefiniteRetry();
    }

}
