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
package com.netflix.astyanax;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;

/**
 * Simple mutation batch using thread local storage to keeps track of one 
 * mutation per thread
 * 
 * @author elandau
 *
 */
public class ThreadLocalMutationBatchManager implements MutationBatchManager {
    private ThreadLocal<MutationBatch> batches = new ThreadLocal<MutationBatch>();
    
    private final Keyspace          keyspace;
    private final ConsistencyLevel  cl;
    private final RetryPolicy       retryPolicy;
    
    public ThreadLocalMutationBatchManager(Keyspace keyspace, ConsistencyLevel cl) {
        this(keyspace, cl, null);
    }
    
    public ThreadLocalMutationBatchManager(Keyspace keyspace, ConsistencyLevel cl, RetryPolicy retryPolicy) {
        this.keyspace    = keyspace;
        this.cl          = cl;
        this.retryPolicy = retryPolicy;
    }
    
    @Override
    public MutationBatch getSharedMutationBatch() {
        MutationBatch mb = batches.get();
        if (mb == null) {
            mb = keyspace.prepareMutationBatch().setConsistencyLevel(cl);
            if (retryPolicy != null) 
                mb.withRetryPolicy(retryPolicy);
            batches.set(mb);
        }
        return mb;
    }
    
    @Override
    public MutationBatch getNewMutationBatch() {
        return keyspace.prepareMutationBatch().setConsistencyLevel(cl);
    }

    @Override
    public void commitSharedMutationBatch() throws ConnectionException {
        MutationBatch mb = batches.get();
        if (mb != null) {
            mb.execute();
            batches.remove();
        }
    }

    @Override
    public void discard() {
        batches.remove();
    }
}
