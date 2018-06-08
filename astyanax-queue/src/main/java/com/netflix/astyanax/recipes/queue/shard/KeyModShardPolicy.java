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
 * Sharding based on the key with fallback to time mod sharding
 * @author elandau
 *
 */
public class KeyModShardPolicy extends TimeModShardPolicy {
    private static KeyModShardPolicy instance = new KeyModShardPolicy();

    public static KeyModShardPolicy getInstance() {
        return instance;
    }
    
    @Override
    public int getMessageShard(Message message, MessageQueueMetadata settings) {
        if (message.hasKey())
            return message.getKey().hashCode() % settings.getShardCount();
        else
            return super.getMessageShard(message, settings);
    }
}
