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
package com.netflix.astyanax.recipes.queue;

import java.util.Collection;

import com.netflix.astyanax.MutationBatch;

/**
 * This interface provides a hook to piggyback on top of the executed mutation
 * for each stage of processing
 * 
 * @author elandau
 *
 */
public interface MessageQueueHooks {
    /**
     * Called after tasks are read from the queue and before the mutation
     * for updating their state is committed.
     * 
     * @param messages
     * @param mb
     */
    void beforeAckMessages(Collection<Message> messages, MutationBatch mb);

    /**
     * Called before a task is released from the queue
     * 
     * @param message
     * @param mb
     */
    void beforeAckMessage(Message message, MutationBatch mb);

    /**
     * Called before a task is inserted in the queue
     * @param message
     * @param mb
     */
    void beforeSendMessage(Message message, MutationBatch mb);
}
