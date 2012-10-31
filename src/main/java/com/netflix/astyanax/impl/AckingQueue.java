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
package com.netflix.astyanax.impl;

import java.util.concurrent.TimeUnit;

import com.netflix.astyanax.MutationBatch;

/**
 * Abstraction for a durable queue requiring an ack to do the final remove
 * 
 * @author elandau
 * 
 */
public interface AckingQueue {
    /**
     * Get the next item from the queue
     * 
     * @param timeout
     * @param units
     * @return
     */
    MutationBatch getNextMutation(long timeout, TimeUnit units) throws InterruptedException;

    /**
     * Insert an item into the queue
     * 
     * @param m
     * @throws Exception
     */
    void pushMutation(MutationBatch m) throws Exception;

    /**
     * Ack a mutation so that it may be removed from the queue
     * 
     * @param m
     */
    void ackMutation(MutationBatch m) throws Exception;

    /**
     * Return a mutation that couldn't be retried for it be requeued and retryed
     * later
     * 
     * @param m
     * @throws Exception
     */
    void repushMutation(MutationBatch m) throws Exception;

    /**
     * Return the number of mutations in the queue
     * 
     * @return
     */
    int size();
}
