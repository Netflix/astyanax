package com.netflix.astyanax.recipes.queue;

import java.util.Collection;

import com.netflix.astyanax.MutationBatch;

public class BaseQueueHook implements MessageQueueHooks {
    @Override
    public void beforeAckMessages(Collection<Message> message, MutationBatch mb) {
    }

    @Override
    public void beforeAckMessage(Message message, MutationBatch mb) {
    }

    @Override
    public void beforeSendMessage(Message message, MutationBatch mb) {
    }
}
