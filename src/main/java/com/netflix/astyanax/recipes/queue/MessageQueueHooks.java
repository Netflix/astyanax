package com.netflix.astyanax.recipes.queue;

import java.util.Collection;

import com.netflix.astyanax.MutationBatch;

/**
 * This interface provides a hook to piggyback on top of the executed mutation
 * for each stage of processing
 * 
 * @author elandau
 *
 */
public interface MessageQueueHooks {
    /**
     * Called after tasks are read from the queue and before the mutation
     * for updating their state is committed.
     * 
     * @param messages
     * @param mb
     */
    void beforeAckMessages(Collection<Message> messages, MutationBatch mb);

    /**
     * Called before a task is released from the queue
     * 
     * @param message
     * @param mb
     */
    void beforeAckMessage(Message message, MutationBatch mb);

    /**
     * Called before a task is inserted in the queue
     * @param message
     * @param mb
     */
    void beforeSendMessage(Message message, MutationBatch mb);
}
