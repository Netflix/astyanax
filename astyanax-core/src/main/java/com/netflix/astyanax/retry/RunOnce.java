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

public class RunOnce implements RetryPolicy {
    public static RunOnce instance = new RunOnce();

    public static RunOnce get() {
        return instance;
    }

    @Override
    public void begin() {
    }

    @Override
    public void success() {
    }

    @Override
    public void failure(Exception e) {
    }

    @Override
    public boolean allowRetry() {
        return false;
    }

    @Override
    public int getAttemptCount() {
        return 1;
    }

    @Override
    public RetryPolicy duplicate() {
        return RunOnce.get();
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

}
