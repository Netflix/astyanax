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

import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Maps;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * Extension to mutation batch which allows for multiple 'named' mutation
 * batches.  The purpose of this manager is to allow mutations to be executed
 * in order of batch creation so that subsequent mutations aren't attempted 
 * if there is a failure.
 * 
 * @author elandau
 *
 */
public class MultiMutationBatchManager implements MutationBatchManager {
    private final String DEFAULT_BATCH_NAME = "default";
    
    private final ThreadLocal<Map<String, MutationBatch>> batches = new ThreadLocal<Map<String, MutationBatch>>();
    
    private final Keyspace keyspace;
    private final ConsistencyLevel cl;
    
    public MultiMutationBatchManager(Keyspace keyspace, ConsistencyLevel cl) {
        this.keyspace = keyspace;
        this.cl = cl;
    }
    
    @Override
    public MutationBatch getSharedMutationBatch() {
        return getNamedMutationBatch(DEFAULT_BATCH_NAME);
    }
    
    public MutationBatch getNamedMutationBatch(String name) {
        Map<String, MutationBatch> mbs = batches.get();
        if (mbs == null) {
            mbs = Maps.newLinkedHashMap();
            batches.set(mbs);
        }
        
        MutationBatch mb = mbs.get(name);
        if (mb == null) {
            mb = getNewMutationBatch();
            mbs.put(name, mb);
        }
        return mb;
    }

    @Override
    public void commitSharedMutationBatch() throws ConnectionException {
        Map<String, MutationBatch> mbs = batches.get();
        if (mbs != null) {
            for (Entry<String, MutationBatch> entry : mbs.entrySet()) {
                entry.getValue().execute();
            }
            batches.remove();
        }
    }

    @Override
    public void discard() {
        batches.remove();
    }

    @Override
    public MutationBatch getNewMutationBatch() {
        return keyspace.prepareMutationBatch().setConsistencyLevel(cl);
    }
}
