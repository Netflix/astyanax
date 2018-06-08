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
package com.netflix.astyanax.contrib.valve;

import java.util.concurrent.atomic.AtomicLong;

public class TimeWindowValve {

    public static enum RequestStatus { 
        OverQuota, PastWindow, Permitted 
    };
    
    final AtomicLong limitValve;
    final Long startWindow; 
    final Long endWindow; 
    
    public TimeWindowValve(final Long limit, final Long startMilliseconds, final long windowMilliseconds) {
        
        limitValve = new AtomicLong(limit);
        startWindow = startMilliseconds;
        endWindow = startWindow + windowMilliseconds;
    }
    
    
    public RequestStatus decrementAndCheckQuota() {
       
        long currentTime = System.currentTimeMillis(); 
        if (currentTime > endWindow) {
            return RequestStatus.PastWindow;
        }
        
        if (limitValve.get() <= 0) {
            return RequestStatus.OverQuota; // this valve is done. no more requests
        }
        
        long value = limitValve.decrementAndGet();
        if (value < 0) {
            return RequestStatus.OverQuota;
        } else {
            return RequestStatus.Permitted;
        }
    }
}
