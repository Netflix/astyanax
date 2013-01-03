package com.netflix.astyanax.recipes.queue;

import java.util.Collection;
import java.util.Map;

public class SendMessageResponse {
    /**
     * Map of messageId to Message
     */
    private Map<String, Message> messages;
    
    /**
     * List of messages that are not unique
     */
    private Collection<Message> notUnique;
    
    public SendMessageResponse(Map<String, Message> success, Collection<Message> notUnique) {
        this.messages = success;
        this.notUnique = notUnique;
    }
    
    public Map<String, Message> getMessages() {
        return messages;
    }

    public Collection<Message> getNotUnique() {
        return notUnique;
    }
}
