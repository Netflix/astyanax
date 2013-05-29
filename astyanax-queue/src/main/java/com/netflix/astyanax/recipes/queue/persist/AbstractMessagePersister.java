package com.netflix.astyanax.recipes.queue.persist;

import java.util.Collection;

import com.netflix.astyanax.recipes.queue.MessageContext;
import com.netflix.astyanax.recipes.queue.MessageQueueException;

public abstract class AbstractMessagePersister implements MessagePersister {

    @Override
    public void readMessages(Collection<MessageContext> messages)
            throws MessageQueueException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void postReadMessage(Collection<MessageContext> messages)
            throws MessageQueueException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void preSendMessages(Collection<MessageContext> messages)
            throws MessageQueueException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void ackMessages(Collection<MessageContext> message)
            throws MessageQueueException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void rollback(Collection<MessageContext> messages) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void createStorage() {
        // TODO Auto-generated method stub
        
    }

}
