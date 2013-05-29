package com.netflix.astyanax.recipes.queue.persist;

import java.util.Collection;

import com.netflix.astyanax.recipes.queue.MessageContext;
import com.netflix.astyanax.recipes.queue.MessageQueueException;

/**
 * Interface for mechanism to persist messages.  For more complex data model it may
 * be possible to have multiple persisters called in sequence such that each persists
 * are different portion of the message.  For example, there may be different persisters
 * for queueing, lookup, search, metadata, ...
 * 
 * @author elandau
 */
public interface MessagePersister {
    /**
     * Read data for the messages and return all messages that were successfully read
     * @return Return back the messages with their metadata
     */
    public void readMessages(Collection<MessageContext> messages) throws MessageQueueException;

    /**
     * Perform addition checks after messages have been read 
     * @param messages
     * @throws MessageQueueException
     */
    public void postReadMessage(Collection<MessageContext> messages) throws MessageQueueException;
    
    /**
     * Called before messages are sent.
     * @param messages
     * @throws MessageQueueException
     */
    public void preSendMessages(Collection<MessageContext> messages) throws MessageQueueException;
    
    /**
     * Ack that these messages have been processed
     * @param message
     * @throws MessageQueueException
     */
    public void ackMessages(Collection<MessageContext> message) throws MessageQueueException;
    
    /**
     * Rollback persistence data for these messages
     * 
     * @param messages
     */
    public void rollback(Collection<MessageContext> messages);
    
    /**
     * Create the underlying storage for this persister.  Ignore already exists errors
     */
    public void createStorage();
    
    /**
     * Return the name of the persister
     * @return
     */
    public String getPersisterName();
}
