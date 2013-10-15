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
package com.netflix.astyanax.connectionpool.impl;

import java.util.concurrent.TimeUnit;

import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.OperationResult;

/**
 * Impl for {@link OperationResult} 
 * Tracks operation attempts, operation pinned host and latency associated with the operation. 
 * 
 * @author elandau
 *
 * @param <R>
 */
public class OperationResultImpl<R> implements OperationResult<R> {

    private final Host host;
    private final R result;
    private final long latency;
    private int attemptCount = 0;

    public OperationResultImpl(Host host, R result, long latency) {
        this.host = host;
        this.result = result;
        this.latency = latency;
    }

    @Override
    public Host getHost() {
        return this.host;
    }

    @Override
    public R getResult() {
        return this.result;
    }

    @Override
    public long getLatency() {
        return this.latency;
    }

    @Override
    public long getLatency(TimeUnit units) {
        return units.convert(this.latency, TimeUnit.NANOSECONDS);
    }

    @Override
    public int getAttemptsCount() {
        return attemptCount;
    }

    @Override
    public void setAttemptsCount(int count) {
        this.attemptCount = count;
    }

}
