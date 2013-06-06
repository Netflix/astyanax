package com.netflix.astyanax.recipes.queue.dao;

import java.util.Collection;

import com.netflix.astyanax.recipes.queue.MessageContext;
import com.netflix.astyanax.recipes.queue.entity.MessageMetadataEntry;
import com.netflix.astyanax.recipes.queue.exception.MessageQueueException;

/**
 * DAO for accessing metadata about specific messages
 * @author elandau
 */
public interface MessageMetadataDao {

    /**
     * Create the underlying storage for the message metadata
     */
    public void createStorage();
    
    /**
     * Fill message metadata into the list of MessageContext.
     * The message context is assumed to have the message keys.
     * 
     * @param messages
     */
    public void readMessages(Collection<MessageContext> messages);

    /**
     * Write only the message metadata for theses messages
     * 
     * @param messages
     * @throws MessageQueueException
     */
    public void writeMessages(Collection<MessageContext> messages) throws MessageQueueException;

    /**
     * Return all pending Ids for the message key
     * @param messageKey
     * @return
     * @throws MessageQueueException
     */
    public Collection<MessageMetadataEntry> getMessageIdsForKey(String messageKey) throws MessageQueueException;

    /**
     * Return all the metadata fiels for the message
     * @param messageKey
     * @return
     * @throws MessageQueueException
     */
    public Collection<MessageMetadataEntry> getMetadataForKey(String messageKey) throws MessageQueueException;

    /**
     * Remove a metadata field
     * @param toDelete
     */
    public void deleteMetadata(MessageMetadataEntry toDelete);

    /**
     * Delete all metadata for a message
     * @param messageKey
     */
    public void deleteMessage(String messageKey);

    /**
     * Write a single item of metadata
     * @param mostRecentMessageId
     */
    public void writeMetadata(MessageMetadataEntry entry);
}
