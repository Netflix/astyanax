package com.netflix.astyanax.recipes.queue;

public interface MessageQueueStats {

    void incEmptyPartitionCount();

    void incLockContentionCount();

    void incProcessCount();

    void incReprocessCount();

    void incExpiredLockCount();

    void incSendMessageCount();
    
    void incAckMessageCount();

    void incInvalidMessageCount();
    
    void incPersistError();

    /**
     * Number of shards that were empty when read.  This is normal and
     * a high number can indicate that the pooling interval is too 
     * low or that there are too many shards in the queue.
     * @return
     */
    long getEmptyPartitionCount();
    
    /**
     * Number of lock contention events
     * @return
     */
    long getLockCountentionCount();
    
    /**
     * Number of messages consumed
     * @return
     */
    long getProcessCount();
    
    /**
     * Number of timed out messages. 
     * @return
     */
    long getReprocessCount();
    
    /**
     * Number of expired locks found on a queue shard.  An expired lock indicates
     * that a client crashed before it could unlock a shard when popping events.
     * @return
     */
    long getExpiredLockCount();
    
    /**
     * Number of message acks being sent
     * @return
     */
    long getAckMessageCount();
    
    /**
     * Number of messages send by this producer.  This is not a global total
     * number of messages ever sent on the queue.
     * @return
     */
    long getSendMessageCount();
    
    /**
     * Number of messages that contain invalid data such unfound task class
     * @return
     */
    long getInvalidMessageCount();

    /**
     * Number of storage errors trying to commit a change to the queue.
     * This include popping and acking messages
     * @return
     */
    long getPersistErrorCount();
}
