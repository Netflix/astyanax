package com.netflix.astyanax.recipes.queue;

import java.util.Collection;
import java.util.List;
import java.util.Map;

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
    long getMessageCount() throws MessageQueueException;
    
    /**
     * Clear all messages in the queue
     * @throws MessageQueueException
     */
    void clearMessages() throws MessageQueueException;
    
    /**
     * Create the underlying storage
     * @throws MessageQueueExcewption
     */
    void createStorage() throws MessageQueueException;
    
    /**
     * Destroy the storage associated with this column family
     * @throws MessageQueueException
     */
    void dropStorage() throws MessageQueueException;
    
    /**
     * Create any metadata in the storage necessary for the queue
     * @throws MessageQueueException
     */
    void createQueue() throws MessageQueueException;

    /**
     * Deletes all the rows for this queue.  This will not 
     * delete any 'key' columns'
     * 
     * @throws MessageQueueException
     */
    void deleteQueue() throws MessageQueueException;
    
    /**
     * Read a specific message from the queue.  The message isn't modified or removed from the queue.
     * 
     * @param messageId Message id returned from MessageProducer.sendMessage
     * @return
     * @throws MessageQueueException
     */
    Message peekMessage(String messageId) throws MessageQueueException;

    /**
     * Peek into messages from the queue.  The queue state is not altered by this operation.
     * @param itemsToPeek
     * @return
     * @throws MessageQueueException
     */
    List<Message> peekMessages(int itemsToPeek) throws MessageQueueException;

    /**
     * Read a specific message from the queue.  The message isn't modified or removed from the queue.
     * This operation will require a lookup of key to messageId
     * 
     * @param message Message id returned from MessageProducer.sendMessage
     * @return
     * @throws MessageQueueException
     */
    Message peekMessageByKey(String key) throws MessageQueueException;
    
    /**
     * Return list of pending associated with the key.  
     * 
     * @param key
     * @return
     * @throws MessageQueueException
     */
    List<Message> peekMessagesByKey(String key) throws MessageQueueException;
    
    /**
     * Read history for the specified key
     * @param key
     * @return
     * @throws MessageQueueException
     */
    List<MessageHistory> getKeyHistory(String key, Long startTime, Long endTime, int count) throws MessageQueueException;
    
    /**
     * Delete a specific message from the queue.  
     * @param message
     * @throws MessageQueueException
     */
    void deleteMessage(String messageId) throws MessageQueueException;
    
    /**
     * Delete a message using the specified key.  This operation will require a lookup of key to messageId
     * prior to deleting the message 
     * @param key
     * @return true if any items were deleted
     * @throws MessageQueueException
     */
    boolean deleteMessageByKey(String key) throws MessageQueueException;
    
    /**
     * Delete a set of messages
     * @param messageIds
     * @throws MessageQueueException
     */
    void deleteMessages(Collection<String> messageIds) throws MessageQueueException;
    
    /**
     * Get the counts for each shard in the queue.  This is an estimate.
     * This is an expensive operation and should be used sparingly.
     * @return
     * @throws MessageQueueException
     */
    Map<String, Integer> getShardCounts() throws MessageQueueException;
    
    /**
     * Return a map of shards and their stats for THIS instance of the queue.
     * These counts are only for the lifetime of this instance and are only incremented
     * by operations performed by this instance.  For actual shard sizes 
     * call getShardCounts();
     * @return
     */
    Map<String, MessageQueueShardStats> getShardStats();

    /**
     * Create a consumer of the message queue.  The consumer will have it's own context
     * 
     * @return
     * @throws MessageQueueException
     */
    MessageConsumer createConsumer();

    /**
     * Create a producer of messages for this queue.
     * @return
     * @throws MessageQueueException
     */
    MessageProducer createProducer();
    
    /**
     * Return the queue's unique name
     * @return
     */
    String getName();
}
