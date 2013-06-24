package com.netflix.astyanax.recipes.queue;

import java.util.List;

import com.netflix.astyanax.recipes.queue.exception.MessageQueueException;

public interface MessageQueueManager {
    /**
     * Create a new message queue
     * @param name
     * @return
     */
    public void createMessageQueue(MessageQueueInfo name);
    
    /**
     * Delete a message queue
     * @param name
     */
    public void deleteMessageQueue(String name);
 
    /**
     * List all message queues
     * @return
     */
    public List<MessageQueueInfo> listMessageQueues();

    /**
     * Read the metadata for a message queue
     * @param name
     * @return
     */
    public MessageQueueInfo readQueueInfo(String name);
    
    /**
     * Create the necessary storage for this queue manager.
     * @throws MessageQueueException 
     */
    public void createStorage() throws MessageQueueException;
    
    /**
     * Drop the storage associated with this queue manager and it's queues
     * @throws MessageQueueException
     */
    public void dropStorage() throws MessageQueueException;

}
