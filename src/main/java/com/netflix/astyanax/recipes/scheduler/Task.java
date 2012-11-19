package com.netflix.astyanax.recipes.scheduler;

import java.util.UUID;

public class Task {
    /**
     * Last execution time, this value changes as the task state is transitioned
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

    public Task() {
        
    }
    
    public Task(UUID taskId, String data) {
        this.token = taskId;
        this.data   = data;
    }
    
    public UUID getTaskId() {
        return token;
    }

    public String getData() {
        return data;
    }

    public Task setTaskId(UUID taskId) {
        this.token = taskId;
        return this;
    }

    public Task setData(String data) {
        this.data = data;
        return this;
    }

    public Long getNextTriggerTime() {
        return nextTriggerTime;
    }

    public Task setNextTriggerTime(Long nextTriggerTime) {
        this.nextTriggerTime = nextTriggerTime;
        return this;
    }
    
    
}
