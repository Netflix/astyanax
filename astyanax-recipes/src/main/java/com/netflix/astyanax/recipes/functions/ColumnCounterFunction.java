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
package com.netflix.astyanax.recipes.functions;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Function;
import com.netflix.astyanax.model.Row;

/**
 * Very basic function to count the total number of columns
 * 
 * @author elandau
 *
 * @param <K>
 * @param <C>
 */
public class ColumnCounterFunction<K,C> implements Function<Row<K,C>, Boolean> {

    private final AtomicLong counter = new AtomicLong(0);
    
    @Override
    public Boolean apply(Row<K,C> input) {
        counter.addAndGet(input.getColumns().size());
        return true;
    }
    
    public long getCount() {
        return counter.get();
    }

    public void reset() {
        counter.set(0);
    }

}
