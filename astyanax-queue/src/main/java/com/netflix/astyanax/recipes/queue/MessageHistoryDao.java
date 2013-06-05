package com.netflix.astyanax.recipes.queue;

import java.util.Collection;

import com.netflix.astyanax.recipes.queue.entity.MessageHistoryEntry;

public interface MessageHistoryDao {
    /**
     * Create the underlying storage for the message metadata
     */
    public void createStorage();
    
    /**
     * Write a single history item
     * @param histroy
     */
    void writeHistory(MessageHistoryEntry histroy);
    
    /**
     * Delete all the history items for a message
     * @param key
     */
    void deleteHistory(String key);

    /**
     * Read history items for a message by key
     * 
     * @param key
     * @param startTime
     * @param endTime
     * @param count
     * @return
     */
    Collection<MessageHistoryEntry> readMessageHistory(String key, Long startTime, Long endTime, int count);

}
