package com.netflix.astyanax.recipes.queue.shard;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.netflix.astyanax.recipes.queue.MessageQueueShard;
import com.netflix.astyanax.recipes.queue.MessageQueueSettings;
import com.netflix.astyanax.recipes.queue.MessageQueueShardStats;

public class TimePartitionedShardReaderPolicy implements ShardReaderPolicy {
    private static final String SEPARATOR = ":";
    
    private final MessageQueueSettings          settings;
    private final List<MessageQueueShard>       shards;
    private final Map<String, MessageQueueShardStats> shardStats;
    private LinkedBlockingQueue<MessageQueueShard> workQueue    = Queues.newLinkedBlockingQueue();
    private LinkedBlockingQueue<MessageQueueShard> idleQueue    = Queues.newLinkedBlockingQueue();

    private int currentTimePartition = -1;

    public TimePartitionedShardReaderPolicy(MessageQueueSettings settings) {
        this.settings = settings;
        
        shards = Lists.newArrayListWithCapacity(settings.getPartitionCount() * settings.getShardCount());
        for (int i = 0; i < settings.getPartitionCount(); i++) {
            for (int j = 0; j < settings.getShardCount(); j++) {
                shards.add(new MessageQueueShard(settings.getQueueName() + SEPARATOR + i + SEPARATOR + j, i, j));
            }
        }
        
        shardStats = Maps.newHashMapWithExpectedSize(shards.size());
        for (MessageQueueShard shard : shards) {
            idleQueue.add(shard);
            shardStats.put(shard.getName(),  shard);
        }
    }

    private int getCurrentPartitionIndex() {
        if (settings.getPartitionCount() <= 1) 
            return 0;
        return    (int) ((TimeUnit.MICROSECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                        / settings.getPartitionDuration())%settings.getPartitionCount());
    }
    
    @Override
    public MessageQueueShard nextShard() throws InterruptedException {
        // We transitioned to a new time partition
        int timePartition = getCurrentPartitionIndex();
        if (timePartition != currentTimePartition) {
            synchronized (this) {
                // Double check
                if (timePartition != currentTimePartition) {
                    // Drain the idle queue and transfer all shards from the
                    // current partition to the work queue
                    currentTimePartition = timePartition;
                    List<MessageQueueShard> temp = Lists.newArrayListWithCapacity(idleQueue.size());
                    idleQueue.drainTo(temp);
                    for (MessageQueueShard partition : shards) {
                        if (partition.getPartition() == currentTimePartition) {
                            workQueue.add(partition);
                        }
                        else {
                            idleQueue.add(partition);
                        }
                    }
                }
            }
        }
        
        return workQueue.poll(settings.getPollInterval(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void releaseShard(MessageQueueShard shard, int messagesRead) {
        if (shard.getPartition() != currentTimePartition && messagesRead == 0) {
            idleQueue.add(shard);
        }
        else {
            workQueue.add(shard);
        }
    }

    @Override
    public Collection<MessageQueueShard> listShards() {
        return Collections.unmodifiableList(shards);
    }

    @Override
    public Map<String, MessageQueueShardStats> getShardStats() {
        return shardStats;
    }
}
