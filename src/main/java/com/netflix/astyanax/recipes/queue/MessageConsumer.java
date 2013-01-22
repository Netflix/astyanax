package com.netflix.astyanax.recipes.queue;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.netflix.astyanax.recipes.locks.BusyLockException;

public interface MessageConsumer {
    /**
     * Acquire up to N items from the queue.  Each item must be released by calling ackMessage.
     * 
     * TODO: Items since last process time
     * 
     * @param itemsToRead
     * @return
     * @throws InterruptedException 
     * @throws Exception 
     */
    List<MessageContext> readMessages(int itemsToRead) throws MessageQueueException, BusyLockException, InterruptedException;

    /**
     * Acquire up to N items from the queue.  Each item must be released by calling ackMessage.
     * 
     * @param itemsToRead
     * @param timeout
     * @param units
     * @return
     */
    List<MessageContext> readMessages(int itemsToRead, long timeout, TimeUnit units) throws MessageQueueException, BusyLockException, InterruptedException;
    
    /**
     * Read messages from a known shard
     * 
     * @param shard
     * @param itemsToRead
     * @return
     * @throws BusyLockException 
     * @throws MessageQueueException 
     */
    List<MessageContext> readMessagesFromShard(String shard, int itemsToRead) throws MessageQueueException, BusyLockException;
    
    /**
     * Peek into messages from the queue.  The queue state is not altered by this operation.
     * @param itemsToPop
     * @return
     * @throws MessageQueueException
     */
    Collection<Message> peekMessages(int itemsToPop) throws MessageQueueException;

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
    
}
