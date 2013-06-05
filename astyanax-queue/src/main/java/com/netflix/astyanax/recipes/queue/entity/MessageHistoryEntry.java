package com.netflix.astyanax.recipes.queue.entity;

import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Id;

public class MessageHistoryEntry {
    @Id
    private String messageKey;
    
    @Column
    private UUID timestamp;
    
    @Column
    private MessageHistory history;
    
    @Column
    private String body;

    public MessageHistoryEntry(String messageKey, UUID timestamp,
            MessageHistory history, String body) {
        super();
        this.messageKey = messageKey;
        this.timestamp = timestamp;
        this.history = history;
        this.body = body;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public void setMessageKey(String messageKey) {
        this.messageKey = messageKey;
    }

    public UUID getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(UUID timestamp) {
        this.timestamp = timestamp;
    }

    public MessageHistory getHistory() {
        return history;
    }

    public void setHistory(MessageHistory history) {
        this.history = history;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    @Override
    public String toString() {
        return "MessageHistoryEntry [messageKey=" + messageKey + ", timestamp="
                + timestamp + ", history=" + history + ", body=" + body + "]";
    }
}
