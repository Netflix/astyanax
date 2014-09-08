package com.netflix.astyanax.recipes.queue.shard;

import com.netflix.astyanax.recipes.queue.Message;
import com.netflix.astyanax.recipes.queue.MessageQueueMetadata;

/**
 * Sharding based on time.  This policy assumes that the 
 * next trigger time in the message has a 'unique' incrementing
 * lower bits.
 * 
 * @author elandau
 *
 */
public class TimeModShardPolicy implements ModShardPolicy {
    private static TimeModShardPolicy instance = new TimeModShardPolicy();

    public static ModShardPolicy getInstance() {
        return instance;
    }
    
    @Override
    public int getMessageShard(Message message, MessageQueueMetadata settings) {
        return (int) (message.getTokenTime() % settings.getShardCount());
    }
}
