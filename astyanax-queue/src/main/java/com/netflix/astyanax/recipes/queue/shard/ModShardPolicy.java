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
