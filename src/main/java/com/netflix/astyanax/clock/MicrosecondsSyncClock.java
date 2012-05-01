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
package com.netflix.astyanax.clock;

import com.netflix.astyanax.Clock;

/**
 * Clock which uses a rolling counter to avoid duplicates.
 * 
 * @author Patricio Echague (pechague@gmail.com)
 */
public class MicrosecondsSyncClock implements Clock {
    private static final long serialVersionUID = -4671061000963496156L;
    private static long lastTime = -1;
    private static final long ONE_THOUSAND = 1000L;

    @Override
    public long getCurrentTime() {
        // The following simulates a microseconds resolution by advancing a
        // static counter
        // every time a client calls the createClock method, simulating a tick.
        long us = System.currentTimeMillis() * ONE_THOUSAND;
        // Synchronized to guarantee unique time within and across threads.
        synchronized (MicrosecondsSyncClock.class) {
            if (us > lastTime) {
                lastTime = us;
            }
            else {
                // the time i got from the system is equals or less
                // (hope not - clock going backwards)
                // One more "microsecond"
                us = ++lastTime;
            }
        }
        return us;
    }

    public String toString() {
        return "MicrosecondsSyncClock";
    }

}
