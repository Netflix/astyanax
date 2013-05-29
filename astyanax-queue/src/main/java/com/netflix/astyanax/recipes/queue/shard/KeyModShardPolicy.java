package com.netflix.astyanax.recipes.queue.shard;

import com.netflix.astyanax.recipes.queue.Message;
import com.netflix.astyanax.recipes.queue.MessageQueueInfo;

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
    public int getMessageShard(Message message, MessageQueueInfo settings) {
        if (message.hasKey())
            return message.getKey().hashCode() % settings.getShardCount();
        else
            return super.getMessageShard(message, settings);
    }
}
