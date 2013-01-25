package com.netflix.astyanax.recipes.queue.shard;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.netflix.astyanax.recipes.queue.MessageQueueShard;
import com.netflix.astyanax.recipes.queue.MessageQueueSettings;

public class TimePartitionedShardStrategy implements ShardStrategy {
    private static final String SEPARATOR = ":";
    
    private final MessageQueueSettings          settings;
    private final List<MessageQueueShard>       shards;
    
    private LinkedBlockingQueue<MessageQueueShard> workQueue    = Queues.newLinkedBlockingQueue();
    private LinkedBlockingQueue<MessageQueueShard> idleQueue    = Queues.newLinkedBlockingQueue();

    private int currentTimePartition = -1;

    public TimePartitionedShardStrategy(MessageQueueSettings config) {
        this.settings = config;
        
        shards = Lists.newArrayListWithCapacity(config.getPartitionCount() * config.getShardCount());
        for (int i = 0; i < config.getPartitionCount(); i++) {
            for (int j = 0; j < config.getShardCount(); j++) {
                shards.add(new MessageQueueShard(config.getQueueName() + SEPARATOR + i + SEPARATOR + j, i, j));
            }
        }
        
        for (MessageQueueShard shard : shards) {
            idleQueue.add(shard);
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
}
