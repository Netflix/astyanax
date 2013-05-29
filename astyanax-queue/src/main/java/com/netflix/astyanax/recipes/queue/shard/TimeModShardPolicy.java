package com.netflix.astyanax.recipes.queue.shard;

import com.netflix.astyanax.recipes.queue.Message;
import com.netflix.astyanax.recipes.queue.MessageQueueInfo;

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
    public int getMessageShard(Message message, MessageQueueInfo settings) {
        return (int) (message.getTrigger().getTriggerTime() % settings.getShardCount());
    }
}
