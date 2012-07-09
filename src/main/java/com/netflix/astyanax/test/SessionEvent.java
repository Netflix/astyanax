package com.netflix.astyanax.test;

import java.util.UUID;

import com.netflix.astyanax.annotations.Component;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class SessionEvent {
    @Component
    private String sessionId;

    @Component
    private UUID timestamp;

    public SessionEvent(String sessionId, UUID timestamp) {
        this.sessionId = sessionId;
        this.timestamp = timestamp;
    }

    public SessionEvent() {

    }

    public String getSessionId() {
        return this.sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public UUID getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(UUID timestamp) {
        this.timestamp = timestamp;
    }

    public String toString() {
        return new StringBuilder().append(sessionId).append(':')
                .append(TimeUUIDUtils.getTimeFromUUID(timestamp)).toString();
    }
}