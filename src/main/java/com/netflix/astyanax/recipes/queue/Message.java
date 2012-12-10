package com.netflix.astyanax.recipes.queue;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;
import com.netflix.astyanax.recipes.scheduler.Trigger;

public class Message {
    
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
     * Map of task arguments.
     */
    private Map<String, Object> parameters;
    
    /**
     * Lower value priority tasks get executed first
     */
    private byte priority = 0;
    
    /**
     * Timeout value in seconds
     */
    private Integer timeout = 5;
    
    /**
     * Unique key for this message.
     */
    private String key;

    /**
     * Class name to handle this message
     */
    private String  taskClass;

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

    public Integer getTimeout() {
        if (timeout == null)
            return 0;
        return timeout;
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

    public Message setTimeout(Integer timeout) {
        this.timeout = timeout;
        return this;
    }
    
    public Message setTimeout(Long timeout, TimeUnit units) {
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
    
    public boolean hasKey() {
        return this.key != null;
    }

    public String getTaskClass() {
        return taskClass;
    }

    public Message setTaskClass(String taskClass) {
        this.taskClass = taskClass;
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
        return message;
    }
}
