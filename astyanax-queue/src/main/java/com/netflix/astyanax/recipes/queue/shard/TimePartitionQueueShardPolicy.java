package com.netflix.astyanax.recipes.queue.shard;

import com.netflix.astyanax.recipes.queue.Message;
import com.netflix.astyanax.recipes.queue.MessageQueueInfo;

/**
 * 
 * @author elandau
 */
public class TimePartitionQueueShardPolicy implements QueueShardPolicy {
    private final ModShardPolicy    modShardPolicy;
    private final MessageQueueInfo  queueInfo;
    
    public TimePartitionQueueShardPolicy(ModShardPolicy modShardPolicy, MessageQueueInfo queueInfo) {
        this.modShardPolicy = modShardPolicy;
        this.queueInfo      = queueInfo;
    }
    
    @Override
    public String getShardKey(Message message) {
        long timestamp = message.getTriggerTime();
        int modShard = this.modShardPolicy.getMessageShard(message, queueInfo);
        
        long timePartition;
        if (queueInfo.getPartitionDuration() != null)
            timePartition = (timestamp / queueInfo.getPartitionDuration()) % queueInfo.getPartitionCount();
        else
            timePartition = 0;
        
        return queueInfo.getQueueName() + ":" + timePartition + ":" + modShard;
    }

}
