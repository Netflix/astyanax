package com.netflix.astyanax.recipes.queue;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

public interface MessageProducer {
    /**
     * Schedule a job for execution
     * @param message
     * @return UUID assigned to this message 
     * 
     * @throws MessageQueueException
     */
    UUID sendMessage(Message message) throws MessageQueueException;

    /**
     * Schedule a batch of jobs
     * @param messages
     * @return Map of messages to their assigned UUIDs
     * 
     * @throws MessageQueueException
     */
    Map<Message, UUID> sendMessages(Collection<Message> messages) throws MessageQueueException;
}
