package com.netflix.astyanax.recipes.queue;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;
import com.netflix.astyanax.recipes.queue.triggers.Trigger;

public class Message {
    
    private static final int DEFAULT_TIMEOUT        = 5;
    
    /**
     * Last execution time, this value changes as the task state is transitioned.  
     * The token is a timeUUID and represents the next execution/expiration time
     * within the queue.
     */
    private UUID  token;
    
    /**
     * Execution time for the task in milliseconds
     */
    private Trigger trigger;

    /**
     * Map of message parameters that are stored with the queued item
     */
    private Map<String, Object> parameters;
    
    /**
     * Lower value priority tasks get executed first
     */
    private byte priority = 0;
    
    /**
     * Timeout value in seconds
     */
    private int timeout = DEFAULT_TIMEOUT;
    
    /**
     * Unique key for this message.
     */
    private String key;

    /**
     * Class name to handle this message
     */
    private String  taskClass;

    /**
     * 
     */
    private boolean isKeepHistory = false;

    /**
     * True if the key is expected to be unique
     */
    private boolean hasUniqueKey = false;
    
    public Message() {
        
    }
    
    public Message(UUID token) {
        this.token = token;
    }
    
    public UUID getToken() {
        return token;
    }

    public Message setToken(UUID token) {
        this.token = token;
        return this;
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public Message setTrigger(Trigger trigger) {
        this.trigger = trigger;
        return this;
    }
    
    public boolean hasTrigger() {
        return trigger != null;
    }
    
    public Map<String, Object> getParameters() {
        return parameters;
    }

    public Message setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
        return this;
    }
    
    public Message addParameter(String key, Object value) {
        if (this.parameters == null)
            this.parameters = Maps.newHashMap();
        this.parameters.put(key, value);
        return this;
    }

    public byte getPriority() {
        return priority;
    }

    public int getTimeout() {
        return timeout;
    }
    
    public boolean hasTimeout() {
        return timeout != 0;
    }

    /**
     * Setting priority will NOT work correctly with a future trigger time due to 
     * internal data model implementations.
     * @param priority
     * @return
     */
    public Message setPriority(byte priority) {
        this.priority = priority;
        return this;
    }

    public Message setTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }
    
    public Message setTimeout(long timeout, TimeUnit units) {
        this.timeout = (int)TimeUnit.SECONDS.convert(timeout, units);
        return this;
    }

    public String getKey() {
        return key;
    }

    public Message setKey(String key) {
        this.key = key;
        return this;
    }
    
    public Message setUniqueKey(String key) {
        this.key = key;
        this.hasUniqueKey = true;
        return this;
    }
    
    public boolean hasKey() {
        return this.key != null;
    }

    public boolean hasUniqueKey() {
        return this.key != null && this.hasUniqueKey;
    }

    public String getTaskClass() {
        return taskClass;
    }

    public Message setTaskClass(String taskClass) {
        this.taskClass = taskClass;
        return this;
    }
    
    public boolean hasTaskClass() {
        return this.taskClass != null;
    }
    
    public boolean isKeepHistory() {
        return isKeepHistory;
    }

    public Message setKeepHistory(Boolean isKeepHistory) {
        this.isKeepHistory = isKeepHistory;
        return this;
    }
    
    public Message clone() {
        Message message = new Message();
        message.token       = token;
        message.trigger     = trigger;
        message.parameters  = parameters;
        message.priority    = priority;
        message.timeout     = timeout;
        message.key         = key;
        message.taskClass   = taskClass;
        message.isKeepHistory = isKeepHistory;
        return message;
    }

    @Override
    public String toString() {
        return "Message [token=" + token + ", trigger=" + trigger + ", parameters=" + parameters + ", priority=" + priority
                + ", timeout=" + timeout + ", key=" + key + ", taskClass=" + taskClass + "]";
    }
}
