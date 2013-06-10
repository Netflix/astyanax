package com.netflix.astyanax.recipes.queue;

import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.annotate.JsonIgnore;

import com.google.common.collect.Maps;
import com.netflix.astyanax.recipes.queue.triggers.Trigger;

public class Message {
    
    private static final int  DEFAULT_TIMEOUT_SECONDS = (int) TimeUnit.MILLISECONDS.convert(120, TimeUnit.SECONDS);
    private static final byte DEFAULT_PRIORITY        = 0;
    
    private static String PROPERTY_TASK_CLASS_NAME = "_taskclass";
    
    public enum Options {
        KeepHistory,
        CompactMessage,
        Unique,
        AutoCommitTrigger,
        AutoDeleteKey
    }
    
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
    private EnumSet<Options> options = EnumSet.noneOf(Options.class);
    
    public Message() {
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
        this.options.add(Options.Unique);
        return this;
    }
    
    public boolean hasKey() {
        return this.key != null;
    }

    public boolean hasUniqueKey() {
        return this.key != null && options.contains(Options.Unique);
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
        return options.contains(Options.KeepHistory);
    }

    public Message setKeepHistory(boolean isKeepHistory) {
        options.add(Options.KeepHistory);
        return this;
    }
    
    public Message setAutoDeleteKey(boolean autoDelete) {
        options.add(Options.AutoDeleteKey);
        return this;
    }
    
    public Message setCompact(boolean isCompact) {
        options.add(Options.CompactMessage);
        return this;
    }
    
    public boolean isCompact() {
        return options.contains(Options.CompactMessage);
    }
    
    public boolean isAutoDeleteKey() {
        return options.contains(Options.AutoDeleteKey);
    }
    
    public boolean hasParameters() {
        return this.parameters != null && !this.parameters.isEmpty();
    }
    
    public Message clone() {
        Message message     = new Message();
        message.trigger     = trigger;
        message.parameters  = parameters;
        message.priority    = priority;
        message.timeout     = timeout;
        message.key         = key;
        message.options     = options;
        return message;
    }

    public boolean isAutoCommitTrigger() {
        return this.options.contains(Options.AutoCommitTrigger);
    }

    public Message setAutoCommitTrigger(boolean isAutoCommitTrigger) {
        this.options.add(Options.AutoCommitTrigger);
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
        sb.append(", options=" + options);
        
        sb.append("]");
        return sb.toString();
    }

}
