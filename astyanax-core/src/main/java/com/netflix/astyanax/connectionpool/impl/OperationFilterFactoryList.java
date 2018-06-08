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
package com.netflix.astyanax.connectionpool.impl;

import java.util.List;

import com.google.common.collect.Lists;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationFilterFactory;

/**
 * Uses the decorator pattern to maintain a list of {@link OperationFilterFactory} for the specified {@link Operation}
 * @author elandau
 *
 */
public class OperationFilterFactoryList implements OperationFilterFactory {

    private final List<OperationFilterFactory> filters = Lists.newArrayList();
    
    @Override
    public <R, CL> Operation<R, CL> attachFilter(Operation<R, CL> operation) {
        for (OperationFilterFactory factory : filters) {
            operation = factory.attachFilter(operation);
        }
        return operation;
    }
    
    public OperationFilterFactoryList addFilterFactory(OperationFilterFactory factory) {
        filters.add(factory);
        return this;
    }
}
