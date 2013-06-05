package com.netflix.astyanax.recipes.queue;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.netflix.astyanax.recipes.queue.entity.MessageQueueEntry;
import com.netflix.astyanax.recipes.queue.exception.MessageQueueException;

/**
 * Read messages for a single shard
 * 
 * @author elandau
 *
 */
public interface MessageQueueDao {
    /**
     * Create the underlying storage for the dao
     */
    public void createStorage();

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
    public Collection<MessageContext> readMessages(String shardName, long upToThisTime, TimeUnit timeUnits, int itemsToPeek) throws MessageQueueException;
    
    /**
     * Read a message by the message ID and return a constructed MessageContext.  The 
     * message context may not have the message metadata attached if it is stored remotely.
     * @param messageId
     * @return
     * @throws MessageQueueException
     */
    public MessageContext readMessage(String messageId) throws MessageQueueException;

    /**
     * Write a single message to the queue
     * @param context
     * @throws MessageQueueException
     */
    void writeMessage(MessageContext context) throws MessageQueueException;
    
    /**
     * Write new messges to the queue
     * 
     * @param messages
     * @throws MessageQueueException
     */
    public void writeMessages(Collection<MessageContext> messages) throws MessageQueueException;

    /**
     * Delete a single queue entry from a shard
     * @param ackMessageId
     */
    public void deleteQueueEntry(MessageQueueEntry ackMessageId);
    
    /**
     * Acknowledge messages
     * 
     * @param messages
     * @return
     */
    public void ackMessagesById(Collection<String> messageIds);

    /**
     * Get the counts for each shard in the queue.  This is an estimate.
     * This is an expensive operation and should be used sparingly.
     * @return
     * @throws MessageQueueException
     */
    public Map<String, Integer> getShardCounts() throws MessageQueueException;

    /**
     * Clear all messages from the queue (ONLY).  This will not remove message history
     * or message metadata stored in separate row keys
     * @throws MessageQueueException
     */
    public void clearMessages() throws MessageQueueException;

}
