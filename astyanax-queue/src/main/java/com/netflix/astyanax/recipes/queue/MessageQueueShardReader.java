package com.netflix.astyanax.recipes.queue;

import java.util.Collection;

/**
 * Read messages for a single shard
 * 
 * @author elandau
 *
 */
public interface MessageQueueShardReader {
    /**
     * Peek into messages contained in the shard.  This call does not take trigger time into account
     * and will return messages that are not yet due to be executed
     * @param shardName
     * @param itemsToPop
     * @return
     * @throws MessageQueueException
     */
    public Collection<MessageContext> readMessages(String shardName, int itemsToPeek) throws MessageQueueException;
    
    /**
     * Peek into messages contained in the shard up to the specified time.
     * @param shardName
     * @param now
     * @param itemsToPeek
     * @return
     * @throws MessageQueueException
     */
    public Collection<MessageContext> readMessages(String shardName, long upToThisTime, int itemsToPeek) throws MessageQueueException;
    
    /**
     * Acknowledge messages
     * 
     * @param messages
     * @return
     */
    public void ackMessagesById(Collection<String> messageIds);
    
}
