package com.netflix.astyanax.recipes.queue.shard;

import java.util.Collection;
import java.util.Map;

import com.netflix.astyanax.recipes.queue.MessageQueueShard;
import com.netflix.astyanax.recipes.queue.MessageQueueShardStats;

/**
 * Policy for scheduling shards to be processed
 *
 * @author elandau
 *
 */
public interface ShardReaderPolicy {
    /**
     * Acquire the next shard to be processed.  Must call releaseShard when done reading
     * from the shard
     * @return
     * @throws InterruptedException
     */
    MessageQueueShard nextShard() throws InterruptedException;

    /**
     * Release a shard after acquiring and reading messages
     * @param shard
     */
    void releaseShard(MessageQueueShard shard, int messagesRead);

    /**
     * List all the shards
     * @return
     */
    Collection<MessageQueueShard> listShards();

    /**
     * Return map of all shard stats
     */
    Map<String, MessageQueueShardStats> getShardStats();

    /**
     * @return number of shards in the work or active queue
     */
    int getWorkQueueDepth();

    /**
     * @return number of shards in the idle queue
     */
    int getIdleQueueDepth();

    /**
     * a ShardReaderPolicy is in catch up mode when more than two time buckets are in the work or active queue.
     * @return is the shard reader catching up.
     */
    boolean isCatchingUp();

    /**
     * @return the correct polling interval.  if in catch up mode returns the catchUpPollInterval otherwise
     * returns the "normal" pollInterval
     */
    long getPollInterval();
}
