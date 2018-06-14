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

public interface MessageProducer {
    /**
     * Schedule a job for execution
     * @param message
     * @return UUID assigned to this message 
     * 
     * @throws MessageQueueException
     */
    String sendMessage(Message message) throws MessageQueueException;

    /**
     * Schedule a batch of jobs
     * @param messages
     * @return Map of messages to their assigned UUIDs
     * 
     * @throws MessageQueueException
     */
    SendMessageResponse sendMessages(Collection<Message> messages) throws MessageQueueException;
}
