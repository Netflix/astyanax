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
package com.netflix.astyanax.test;

import com.google.common.base.Supplier;

public class IncreasingRateSupplier implements Supplier<Integer>{
    private int rate;
    private final long delta;
    
    public IncreasingRateSupplier(int initialRate, int delta) {
        this.rate = initialRate;
        this.delta = delta;
    }
    
    public Integer get() {
        rate += this.delta;
        return rate;
    }
}
