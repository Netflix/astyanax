package com.netflix.astyanax.recipes.queue;

import java.util.Collection;
import java.util.Map;

public class PersistMessageResponse {
    /**
     * Map of messageId to Message
     */
    private Collection<MessageContext> messages;
    
    /**
     * List of messages that are not unique
     */
    private Map<Message, String> badMessages;
    
    public PersistMessageResponse(Collection<MessageContext> messages, Map<Message, String> badMessages) {
        this.messages = messages;
        this.badMessages = badMessages;
    }

    public Collection<MessageContext> getMessages() {
        return messages;
    }

    public Map<Message, String> getBadMessages() {
        return badMessages;
    }
}
