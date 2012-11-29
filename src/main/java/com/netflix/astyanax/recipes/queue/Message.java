package com.netflix.astyanax.recipes.queue;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Message {
    /**
     * Last execution time, this value changes as the task state is transitioned.  
     * The token is a timeUUID and represents the next execution/expiration time
     * within the queue.
     */
    private UUID    token;
    
    /**
     * Data associated with the task
     */
    private String  data;
    
    /**
     * Execution time for the task in milliseconds
     */
    private Long    nextTriggerTime;

    /**
     * Map of task arguments.
     */
    private Map<String, Object> parameters;
    
    /**
     * Lower value priority tasks get executed first
     */
    private short priority = 0;
    
    /**
     * Timeout value in seconds
     */
    private Integer timeout = 10;
    
    public Message() {
        
    }
    
    public Message(String data) {
        this.data = data;
    }
    
    public Message(UUID token, String data) {
        this.token = token;
        this.data   = data;
    }
    
    public UUID getToken() {
        return token;
    }

    public String getData() {
        return data;
    }

    public Message setToken(UUID token) {
        this.token = token;
        return this;
    }

    public Message setData(String data) {
        this.data = data;
        return this;
    }

    public Long getNextTriggerTime() {
        if (nextTriggerTime == null)
            return 0L;
        return nextTriggerTime;
    }

    public Message setNextTriggerTime(Long nextTriggerTime) {
        this.nextTriggerTime = nextTriggerTime;
        return this;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    public short getPriority() {
        return priority;
    }

    public Integer getTimeout() {
        if (timeout == null)
            return 0;
        return timeout;
    }

    public Message setPriority(short priority) {
        this.priority = priority;
        return this;
    }

    public Message setTimeout(Integer timeout) {
        this.timeout = timeout;
        return this;
    }
    
    public Message setTimeout(Long timeout, TimeUnit units) {
        this.timeout = (int)TimeUnit.SECONDS.convert(timeout, units);
        return this;
    }
    
    
}
