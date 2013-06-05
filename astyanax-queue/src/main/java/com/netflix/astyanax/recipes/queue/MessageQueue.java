package com.netflix.astyanax.recipes.queue;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.netflix.astyanax.recipes.locks.BusyLockException;
import com.netflix.astyanax.recipes.queue.entity.MessageHistoryEntry;
import com.netflix.astyanax.recipes.queue.exception.MessageQueueException;

/**
 * Base interface for a distributed message queue.
 * 
 * Common use pattern
 * 
 *  MessageQueue queue = ...;
 *  List<Message> messages = queue.readMessages(10);
 *  for (Message message : messages) {
 *      try {
 *          // Do something with this message
 *      }
 *      finally {
 *          queue.ackMessage(message);
 *      }
 *  }
 *  
 * @author elandau
 *
 */
public interface MessageQueue {
    /**
     * Return the number of messages in the queue.  This is an estimate.
     * This is an expensive operation and should be used sparingly.
     * @return  Number of messages, including messages currently being processed
     */
    public long getMessageCount() throws MessageQueueException;
    
    /**
     * Clear all messages in the queue
     * @throws MessageQueueException
     */
    public void clearMessages() throws MessageQueueException;
    
    /**
     * Read a specific message from the queue.  The message isn't modified or removed from the queue.
     * 
     * @param messageId Message id returned from MessageProducer.sendMessage
     * @return
     * @throws MessageQueueException
     */
    public MessageContext peekMessage(String messageId) throws MessageQueueException;

    /**
     * Peek into messages from the queue.  The queue state is not altered by this operation.
     * @param itemsToPeek
     * @return
     * @throws MessageQueueException
     */
    public Collection<MessageContext> peekMessages(int itemsToPeek) throws MessageQueueException;

    /**
     * Return list of pending associated with the key.  
     * 
     * @param key
     * @return
     * @throws MessageQueueException
     */
    public Collection<MessageContext> peekMessagesByKey(String key) throws MessageQueueException;
    
    /**
     * Read history for the specified key
     * @param key
     * @return
     * @throws MessageQueueException
     */
    public Collection<MessageHistoryEntry> getKeyHistory(String key, Long startTime, Long endTime, int count) throws MessageQueueException;
    
    /**
     * Delete a specific message from the queue.  
     * @param message
     * @throws MessageQueueException
     */
    public void deleteMessage(String messageId) throws MessageQueueException;
    
    /**
     * Delete a message using the specified key.  This operation will require a lookup of key to messageId
     * prior to deleting the message 
     * @param key
     * @return true if any items were deleted
     * @throws MessageQueueException
     */
    public boolean deleteMessageByKey(String key) throws MessageQueueException;
    
    /**
     * Delete a set of messages
     * @param messageIds
     * @throws MessageQueueException
     */
    public void deleteMessages(Collection<String> messageIds) throws MessageQueueException;
    
    /**
     * Get the counts for each shard in the queue.  This is an estimate.
     * This is an expensive operation and should be used sparingly.
     * @return
     * @throws MessageQueueException
     */
    public Map<String, Integer> getShardCounts() throws MessageQueueException;
    
    /**
     * Return a map of shards and their stats for THIS instance of the queue.
     * These counts are only for the lifetime of this instance and are only incremented
     * by operations performed by this instance.  For actual shard sizes 
     * call getShardCounts();
     * @return
     */
    public Map<String, MessageQueueShardStats> getShardStats();

    /**
     * Return the queue's unique name
     * @return
     */
    public String getName();

    /**
     * Pop messages from the queue
     * @param itemsToPop
     * @return
     * @throws MessageQueueException
     * @throws BusyLockException
     * @throws InterruptedException
     */
    public Collection<MessageContext> readMessages(int itemsToPop) throws MessageQueueException, BusyLockException, InterruptedException;

    /**
     * Read messages from the queue with a given timeout
     * 
     * @param itemsToPop
     * @param timeout
     * @param units
     * @return
     * @throws MessageQueueException
     * @throws BusyLockException
     * @throws InterruptedException
     */
    public Collection<MessageContext> readMessages(int itemsToPop, long timeout, TimeUnit units) throws MessageQueueException, BusyLockException,  InterruptedException;

    /**
     * Release a job after completion
     * @param item
     * @throws Exception 
     */
    void ackMessage(MessageContext message) throws MessageQueueException;

    /**
     * Release a set of jobs
     * @param items
     */
    void ackMessages(Collection<MessageContext> messages) throws MessageQueueException;

    /**
     * Acknowledge the message as a poison message.  This will put the message into
     * a poison queue so it is persisted but does not interfere with the active queue.
     * 
     * @param message
     */
    void ackPoisonMessage(MessageContext message) throws MessageQueueException;

    /**
     * Schedule a job for execution
     * 
     * @param message
     * @return UUID assigned to this message 
     * 
     * @throws MessageQueueException
     */
    MessageContext sendMessage(Message message) throws MessageQueueException;

    /**
     * Schedule a batch of jobs
     * @param messages
     * @return Map of messages to their assigned UUIDs
     * 
     * @throws MessageQueueException
     */
    Collection<MessageContext> sendMessages(Collection<Message> messages) throws MessageQueueException;    
}