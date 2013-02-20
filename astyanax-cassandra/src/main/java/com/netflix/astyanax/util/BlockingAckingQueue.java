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
package com.netflix.astyanax.util;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.impl.AckingQueue;

public class BlockingAckingQueue implements AckingQueue {

    private LinkedBlockingQueue<MutationBatch> queue = Queues.newLinkedBlockingQueue();
    private ConcurrentMap<MutationBatch, Boolean> busy = Maps.newConcurrentMap();

    @Override
    public MutationBatch getNextMutation(long timeout, TimeUnit unit) throws InterruptedException {
        MutationBatch mutation = queue.poll(timeout, unit);
        if (mutation != null) {
            busy.put(mutation, true);
        }
        return mutation;
    }

    @Override
    public void pushMutation(MutationBatch m) throws Exception {
        queue.put(m);
    }

    @Override
    public void ackMutation(MutationBatch m) throws Exception {
        busy.remove(m);
    }

    @Override
    public void repushMutation(MutationBatch m) throws Exception {
        busy.remove(m);
        pushMutation(m);
    }

    @Override
    public int size() {
        return queue.size();
    }
}
