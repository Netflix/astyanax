package com.netflix.astyanax.recipes.queue;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.annotate.JsonIgnore;

import com.google.common.collect.Maps;
import com.netflix.astyanax.recipes.queue.triggers.Trigger;

public class Message {
    
    private static final int  DEFAULT_TIMEOUT_SECONDS = (int) TimeUnit.MILLISECONDS.convert(120, TimeUnit.SECONDS);
    private static final byte DEFAULT_PRIORITY        = 0;
    
    private static String PROPERTY_TASK_CLASS_NAME = "_taskclass";
    
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
    private byte priority = DEFAULT_PRIORITY;
    
    /**
     * Timeout value in seconds
     */
    private int timeout = DEFAULT_TIMEOUT_SECONDS;
    
    /**
     * Unique key for this message.
     */
    private String key;

    /**
     * Boolean options.  See Options enum above
     */
    private boolean keepHistory = false;
    
    private boolean isCompactMessage = false;
    
    private boolean isUniqueKey = false;
    
    private boolean isAutoCommitTrigger = false;
    
    private boolean isAutoDeleteKey = false;
    
    public Message() {
    }
    
    public Message(Message message, Trigger trigger) {
        this.key                 = message.key;
        this.keepHistory         = message.keepHistory;
        this.isCompactMessage    = message.isCompactMessage;
        this.isUniqueKey         = message.isUniqueKey;
        this.isAutoCommitTrigger = message.isAutoCommitTrigger;
        this.isAutoDeleteKey     = message.isAutoDeleteKey;
        this.parameters          = message.parameters;
        this.priority            = message.priority;
        this.trigger             = trigger;
        this.timeout             = message.timeout;
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public Message setTrigger(Trigger trigger) {
        this.trigger = trigger;
        return this;
    }
    
    @JsonIgnore
    public long getTriggerTime() {
        if (this.trigger == null)
            return System.currentTimeMillis();
        else
            return this.trigger.getTriggerTime();
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

    public Message setTimeout(long timeout, TimeUnit units) {
        this.timeout = (int)TimeUnit.MILLISECONDS.convert(timeout, units);
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
        this.isUniqueKey = true;
        return this;
    }
    
    public boolean hasKey() {
        return this.key != null;
    }

    public boolean hasUniqueKey() {
        return this.key != null && isUniqueKey;
    }

    public String getTaskClass() {
        if (parameters == null)
            return null;
        return (String)parameters.get(PROPERTY_TASK_CLASS_NAME);
    }

    public Message setTaskClass(String taskClass) {
        if (parameters == null) {
            parameters = Maps.newHashMap();
        }
        
        parameters.put(PROPERTY_TASK_CLASS_NAME, taskClass);
        return this;
    }
    
    public boolean hasTaskClass() {
        return getTaskClass() != null;
    }
    
    public boolean isKeepHistory() {
        return this.keepHistory;
    }

    public Message setKeepHistory(boolean isKeepHistory) {
        keepHistory = isKeepHistory;
        return this;
    }
    
    public Message setAutoDeleteKey(boolean autoDelete) {
        this.isAutoDeleteKey = autoDelete;
        return this;
    }
    
    public Message setCompact(boolean isCompact) {
        this.isCompactMessage = isCompact;
        return this;
    }
    
    public boolean isCompact() {
        return this.isCompactMessage;
    }
    
    public boolean isAutoDeleteKey() {
        return this.isAutoDeleteKey;
    }
    
    public boolean hasParameters() {
        return this.parameters != null && !this.parameters.isEmpty();
    }

    public boolean isAutoCommitTrigger() {
        return this.isAutoCommitTrigger;
    }

    public Message setAutoCommitTrigger(boolean isAutoCommitTrigger) {
        this.isAutoCommitTrigger = isAutoCommitTrigger;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Message[");
        if (trigger != null)
            sb.append(", trigger=" + trigger);
        if (parameters != null)
            sb.append(", parameters=" + parameters);           
        sb.append(", priority=" + priority);
        sb.append(", timeout=" + timeout);
        if (key != null)
            sb.append(", key=" + key);
        
        sb.append("]");
        return sb.toString();
    }

}
