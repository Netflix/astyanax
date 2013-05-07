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
import com.netflix.astyanax.recipes.queue.MessageQueueMetadata;
import com.netflix.astyanax.recipes.queue.MessageQueueShard;
import com.netflix.astyanax.recipes.queue.MessageQueueShardStats;

public class TimePartitionedShardReaderPolicy implements ShardReaderPolicy {
    public static final long DEFAULT_POLLING_INTERVAL  = 1000;
    public static final long NO_CATCHUP_POLLING_INTERVAL = 0;
    
    public static class Factory implements ShardReaderPolicy.Factory {
        public static class Builder {
            private long pollingInterval        = DEFAULT_POLLING_INTERVAL;
            private long catchupPollingInterval = NO_CATCHUP_POLLING_INTERVAL;
            
            public Builder withPollingInterval(long pollingInterval, TimeUnit units) {
                this.pollingInterval = TimeUnit.MILLISECONDS.convert(pollingInterval, units);
                return this;
            }
            
            public Builder withCatchupPollingInterval(long catchupPollingInterval, TimeUnit units) {
                this.catchupPollingInterval = TimeUnit.MILLISECONDS.convert(catchupPollingInterval, units);
                return this;
            }
            
            public Factory build() {
                return new Factory(this);
            }
        }
        
        public static Builder builder() {
            return new Builder();
        }
        
        public Factory(Builder builder) {
            this.builder = builder;
        }
        
        private final Builder builder;
                
        @Override
        public ShardReaderPolicy create(MessageQueueMetadata metadata) {
            return new TimePartitionedShardReaderPolicy(builder, metadata);
        }
    }
    
    private static final String SEPARATOR = ":";
    
    private final MessageQueueMetadata                   settings;
    private final List<MessageQueueShard>                shards;
    private final Map<String, MessageQueueShardStats>    shardStats;
    private final LinkedBlockingQueue<MessageQueueShard> workQueue = Queues.newLinkedBlockingQueue();
    private final LinkedBlockingQueue<MessageQueueShard> idleQueue = Queues.newLinkedBlockingQueue();
    private final long pollingInterval;
    private final long catchupPollingInterval;

    private int currentTimePartition = -1;

    private TimePartitionedShardReaderPolicy(Factory.Builder builder, MessageQueueMetadata metadata) {
        this.settings               = metadata;
        this.pollingInterval        = builder.pollingInterval;
        this.catchupPollingInterval = builder.catchupPollingInterval;
        
        shards = Lists.newArrayListWithCapacity(metadata.getPartitionCount() * metadata.getShardCount());
        for (int i = 0; i < metadata.getPartitionCount(); i++) {
            for (int j = 0; j < metadata.getShardCount(); j++) {
                shards.add(new MessageQueueShard(metadata.getQueueName() + SEPARATOR + i + SEPARATOR + j, i, j));
            }
        }
        
        List<MessageQueueShard> queues = Lists.newArrayList();
        shardStats = Maps.newHashMapWithExpectedSize(shards.size());
        for (MessageQueueShard shard : shards) {
            queues.add(shard);
            shardStats.put(shard.getName(),  shard);
        }
        
        Collections.shuffle(queues);
        workQueue.addAll(queues);
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
                    currentTimePartition = timePartition;
                    
                    // Drain the idle queue and transfer all shards from the
                    // current partition to the work queue
                    List<MessageQueueShard> temp = Lists.newArrayListWithCapacity(idleQueue.size());
                    idleQueue.drainTo(temp);
                    for (MessageQueueShard partition : temp) {
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
        
        // This should only block if we have more client threads than mod shards in the queue,
        // which we would expect to be the case
        return workQueue.take();
    }

    @Override
    public void releaseShard(MessageQueueShard shard, int messagesRead) {
        // Shard is not in the current partition and we did't final any messages so let's just put in the
        // idle queue.  It'll be added back later when in this shard's time partition.
        // May want to randomly check an idle queue when there is nothing in the working queue
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

    @Override
    public int getWorkQueueDepth() {
        return workQueue.size();
    }

    @Override
    public int getIdleQueueDepth() {
        return idleQueue.size();
    }

    @Override
    public boolean isCatchingUp() {
        // if the work queue is larger than two partitions worth of shards we are still playing catch up.
        return getWorkQueueDepth() > (settings.getShardCount() * 2);
    }

    @Override
    public long getPollInterval() {
        return (isCatchingUp() && catchupPollingInterval != NO_CATCHUP_POLLING_INTERVAL )
                    ? catchupPollingInterval 
                    : pollingInterval;
    }
}
