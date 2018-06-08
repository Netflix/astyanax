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
package com.netflix.astyanax.contrib.dualwrites;

import java.util.Collection;

import com.netflix.astyanax.Execution;

/**
 * Interface for dealing with 2 separate executions one is the primary, the other is the secondary. 
 * There are several possible strategies 
 * 
 * 1. BEST EFFORT - Do first, fail everything if it fails. The try second and just log it if it fails.
 * 2. FAIL ON ALL - try both and fail if any of them FAIL. 
 * 3. BEST EFFORT ASYNC - similar to the 1st but try the 2nd write in a separate thread and do not block the caller. 
 * 4. PARALLEL WRITES - similar to 2. but try both in separate threads, so that we don't pay for the penalty of dual writes latency as in the SEQUENTIAL method.
 *  
 * @author poberai
 *
 */
public interface DualWritesStrategy {

    /**
     * 
     * @param primary
     * @param secondary
     * @param writeMetadata
     * @return
     */
    public <R> Execution<R> wrapExecutions(Execution<R> primary, Execution<R> secondary, Collection<WriteMetadata> writeMetadata); 

    /**
     * 
     * @return FailedWritesLogger
     */
    public FailedWritesLogger getFailedWritesLogger();
}
