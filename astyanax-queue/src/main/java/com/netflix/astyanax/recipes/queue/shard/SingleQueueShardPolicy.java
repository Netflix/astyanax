package com.netflix.astyanax.recipes.queue.shard;

import com.netflix.astyanax.recipes.queue.Message;
import com.netflix.astyanax.recipes.queue.MessageQueueInfo;

/**
 * Shard policy for a single fixed name shard regardless of message content
 * 
 * @author elandau
 *
 */
public class SingleQueueShardPolicy implements QueueShardPolicy {
    private final String shardName;
    
    public SingleQueueShardPolicy(MessageQueueInfo queueInfo, String shardName) {
        this.shardName = queueInfo.getQueueName() + ":" + shardName;
    }

    @Override
    public String getShardKey(Message message) {
        return shardName;
    }
    
}
