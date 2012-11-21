package com.netflix.astyanax.recipes.scheduler;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Task {
    /**
     * Last execution time, this value changes as the task state is transitioned.  
     * The token is a timeUUID and represents the next execution/expiration time
     * within the scheduler.
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
     * Flag indicating whether the job can be re-run on timeout
     */
    private Boolean rerunnable;
    
    /**
     * Number of iterations.  Must be set in conjunction with 'interval'.
     * If set to 0 then repeat indefinitely
     */
    private Long iterationCount;
    
    /**
     * Lower value priority tasks get executed first
     */
    private short priority = 0;
    
    /**
     * Interval between execution.  Must be set in conjunction with 'iterationCount'.  
     */
    private Long interval;
    
    /**
     * Timeout value in seconds
     */
    private Integer timeout = 10;
    
    public Task() {
        
    }
    
    public Task(String data) {
        this.data = data;
    }
    
    public Task(UUID token, String data) {
        this.token = token;
        this.data   = data;
    }
    
    public UUID getToken() {
        return token;
    }

    public String getData() {
        return data;
    }

    public Task setToken(UUID token) {
        this.token = token;
        return this;
    }

    public Task setData(String data) {
        this.data = data;
        return this;
    }

    public Long getNextTriggerTime() {
        if (nextTriggerTime == null)
            return 0L;
        return nextTriggerTime;
    }

    public Task setNextTriggerTime(Long nextTriggerTime) {
        this.nextTriggerTime = nextTriggerTime;
        return this;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public Boolean getRerunnable() {
        return rerunnable;
    }

    public Long getIterationCount() {
        return iterationCount;
    }

    public Long getInterval() {
        return interval;
    }

    public void setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    public void setRerunnable(Boolean rerunnable) {
        this.rerunnable = rerunnable;
    }

    public void setIterationCount(Long iterationCount) {
        this.iterationCount = iterationCount;
    }

    public void setInterval(Long interval) {
        this.interval = interval;
    }

    public short getPriority() {
        return priority;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public Task setPriority(short priority) {
        this.priority = priority;
        return this;
    }

    public Task setTimeout(Integer timeout) {
        this.timeout = timeout;
        return this;
    }
    
    public Task setTimeout(Long timeout, TimeUnit units) {
        this.timeout = (int)TimeUnit.SECONDS.convert(timeout, units);
        return this;
    }
    
    
}
