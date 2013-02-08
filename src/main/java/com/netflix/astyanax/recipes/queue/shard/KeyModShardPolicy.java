package com.netflix.astyanax.recipes.queue.shard;

import com.netflix.astyanax.recipes.queue.Message;

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
    public int getMessageShard(Message message, int shardCount) {
        if (message.hasKey())
            return message.getKey().hashCode() % shardCount;
        return super.getMessageShard(message, shardCount);
    }
}
