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
package com.netflix.astyanax.recipes.queue.shard;

import com.netflix.astyanax.recipes.queue.Message;
import com.netflix.astyanax.recipes.queue.MessageQueueMetadata;

/**
 * Policy for mod sharding within a time partition
 * 
 * @author elandau
 *
 */
public interface ModShardPolicy {
    /**
     * Return the mod shard for the specified message.  The shard can be based
     * on any message attribute such as the schedule time or the message key
     * @param message
     * @return
     */
    int getMessageShard(Message message, MessageQueueMetadata settings);
}
