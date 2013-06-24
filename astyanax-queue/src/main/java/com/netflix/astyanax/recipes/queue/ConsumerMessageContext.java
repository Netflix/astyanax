package com.netflix.astyanax.recipes.queue;

/**
 * Abstraction passed to consumers of the message queue.
 * 
 * This lets the consumer get the actual message but also specify the next message to run
 * when the message is part of a workflow
 * 
 * @author elandau
 *
 */
public interface ConsumerMessageContext {
    /**
     * @return Get the dequeued message
     */
    Message getMessage();

    /**
     * Set the next message to execute.
     * 
     * @param nextMessage
     * @return
     * 
     * TODO: Determine how this will work with recurring events.  Specifically, recurring events
     *       with autoCommit.  We may disallow setNextMessage for messages with autoCommit. 
     *       For recurring events this may end up replacing the auto-generated event.
     */
    void setNextMessage(Message nextMessage);

    /**
     * 
     * @return
     */
    Message getNextMessage();

}
