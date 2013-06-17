package com.netflix.astyanax.recipes.queue.shard;

import java.util.UUID;

import com.netflix.astyanax.recipes.queue.Message;
import com.netflix.astyanax.recipes.queue.MessageQueueInfo;
import com.netflix.astyanax.recipes.queue.dao.MessageQueueDao;
import com.netflix.astyanax.util.TimeUUIDUtils;

/**
 * Sharding policy where each client produces up to N messages into a shard
 * and a single shard.  All shards are tracked in a single row.
 * 
 * @author elandau
 *
 */
public class ClientPartitionedShardPolicy implements QueueShardPolicy {
    private final ShardDirectoryDao shardDirectoryDao;
    private final int               maxItemsPerShard;
    private int                     counter;
    private UUID                    shardId;
    
    public ClientPartitionedShardPolicy(
            MessageQueueInfo  queueInfo,
            ShardDirectoryDao shardDirectoryDao,
            MessageQueueDao   queueDao,
            int               maxItemsPerShard) {
        this.maxItemsPerShard  = maxItemsPerShard;
        this.counter           = maxItemsPerShard;
        this.shardDirectoryDao = shardDirectoryDao;
    }

    @Override
    public synchronized String getShardKey(Message message) {
        if (++counter > maxItemsPerShard) {
            // Finalize the current shard to indicate that no more meesages will be written to it
            shardDirectoryDao.finalizeShard(shardId.toString());
            
            // Add a new shard to the directory
            shardId = TimeUUIDUtils.getUniqueTimeUUIDinMicros();
            shardDirectoryDao.registerShard(shardId.toString(), System.currentTimeMillis());
        }
        
        return shardId.toString();
    }

    @Override
    public void discardShard(String shardName) {
        shardDirectoryDao.discardShard(shardName);
    }
    
}
