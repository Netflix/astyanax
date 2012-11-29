package com.netflix.astyanax.recipes.queue;

import java.util.Collection;

import com.netflix.astyanax.recipes.locks.BusyLockException;

public interface MessageConsumer {
    /**
     * Acquire N items from the queue.  Each item must be released
     * 
     * TODO: Items since last process time
     * 
     * @param itemsToPop
     * @return
     * @throws InterruptedException 
     * @throws Exception 
     */
    Collection<Message> readMessages(int itemsToPop) throws MessageQueueException, BusyLockException, InterruptedException;

    /**
     * Release a job after completion
     * @param item
     * @throws Exception 
     */
    void ackMessage(Message message) throws MessageQueueException;

    /**
     * Release a set of jobs
     * @param items
     */
    void ackMessages(Collection<Message> messages) throws MessageQueueException;
    
}
