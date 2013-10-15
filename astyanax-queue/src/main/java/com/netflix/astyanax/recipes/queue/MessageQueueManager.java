package com.netflix.astyanax.recipes.queue;

import java.util.List;

public interface MessageQueueManager {
    /**
     * Create a new message queue
     * @param name
     * @return
     */
    MessageQueue createMessageQueue(MessageQueueMetadata name);
    
    /**
     * Get an existing message queue
     * @param name
     * @return
     */
    MessageQueue getMessageQueue(String name);
    
    /**
     * Delete a message queue
     * @param name
     */
    void deleteMessageQueue(String name);
 
    /**
     * List all message queues
     * @return
     */
    List<MessageQueueMetadata> listMessageQueues();
}
