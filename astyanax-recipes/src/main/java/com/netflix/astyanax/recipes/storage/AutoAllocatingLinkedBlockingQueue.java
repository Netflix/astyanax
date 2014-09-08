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
package com.netflix.astyanax.recipes.storage;

import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.base.Supplier;

@SuppressWarnings("serial")
public class AutoAllocatingLinkedBlockingQueue<T> extends LinkedBlockingQueue<T> {

    public AutoAllocatingLinkedBlockingQueue(int concurrencyLevel) {
        super(concurrencyLevel);
    }

    public T poll(Supplier<T> supplier) {
        T bb = super.poll();
        if (bb == null) {
            return supplier.get();
        }
        return bb;
    }
}
