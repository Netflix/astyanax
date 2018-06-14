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
package com.netflix.astyanax.clock;

import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.astyanax.Clock;

public class MicrosecondsAsyncClock implements Clock {

    private static final long serialVersionUID = -4671061000963496156L;
    private static final long ONE_THOUSAND = 1000L;

    private static AtomicInteger counter = new AtomicInteger(0);

    public MicrosecondsAsyncClock() {

    }

    @Override
    public long getCurrentTime() {
        // The following simulates a microseconds resolution by advancing a
        // static counter
        // every time a client calls the createClock method, simulating a tick.
        long us = System.currentTimeMillis() * ONE_THOUSAND;
        return us + counter.getAndIncrement() % ONE_THOUSAND;
    }

    public String toString() {
        return "MicrosecondsAsyncClock";
    }
}
