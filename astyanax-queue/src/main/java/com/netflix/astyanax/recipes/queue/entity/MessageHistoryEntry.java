package com.netflix.astyanax.recipes.queue.entity;

import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import com.netflix.astyanax.entitystore.TTL;
import com.netflix.astyanax.util.TimeUUIDUtils;

@Entity
public class MessageHistoryEntry {
    @Id
    private String messageKey;
    
    @Column
    private UUID timestamp;
    
    @Column
    private String body;

    @TTL
    private int ttl;
    
    public MessageHistoryEntry(String messageKey, String body) {
        super();
        this.messageKey = messageKey;
        this.timestamp  = TimeUUIDUtils.getUniqueTimeUUIDinMicros();
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

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    @Override
    public String toString() {
        return "MessageHistoryEntry [messageKey=" + messageKey + ", timestamp="
                + timestamp + ", body=" + body + "]";
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }
}
