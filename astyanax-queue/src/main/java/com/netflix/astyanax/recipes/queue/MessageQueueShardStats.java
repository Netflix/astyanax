package com.netflix.astyanax.recipes.queue;

public interface MessageQueueShardStats {
    public long getLastReadCount();
    public long getReadCount();
    public long getWriteCount();
}
