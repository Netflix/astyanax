/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
