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

public class BaseQueueHook implements MessageQueueHooks {
    @Override
    public void beforeAckMessages(Collection<Message> message, MutationBatch mb) {
    }

    @Override
    public void beforeAckMessage(Message message, MutationBatch mb) {
    }

    @Override
    public void beforeSendMessage(Message message, MutationBatch mb) {
    }
}
