package com.netflix.astyanax.recipes.queue;

import java.util.UUID;

/**
 * Track history for a single execution of a task
 * 
 * @author elandau
 *
 */
public class MessageHistory {
    private UUID        token;
    
    /**
     * Time when the task was supposed to be triggered
     */
    private long        triggerTime;
    
    /**
     * Time when the task was actually triggered
     */
    private long        startTime;
    
    /**
     * Time when task processing ended
     */
    private long        endTime;
    
    /**
     * Status of task execution
     */
    private MessageStatus  status;
    
    /**
     * Stack trace in the event that the execution failed
     */
    private String      stackTrace;
    
    /**
     * Error that occured during execution
     */
    private String      error;
    
    public long getTriggerTime() {
        return triggerTime;
    }
    public void setTriggerTime(long triggerTime) {
        this.triggerTime = triggerTime;
    }
    public long getStartTime() {
        return startTime;
    }
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }
    public long getEndTime() {
        return endTime;
    }
    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }
    public MessageStatus getStatus() {
        return status;
    }
    public void setStatus(MessageStatus status) {
        this.status = status;
    }
    public String getStackTrace() {
        return stackTrace;
    }
    public void setStackTrace(String stackTrace) {
        this.stackTrace = stackTrace;
    }
    public String getError() {
        return error;
    }
    public void setError(String exception) {
        this.error = exception;
    }
    public UUID getToken() {
        return token;
    }
    public void setToken(UUID token) {
        this.token = token;
    }
    @Override
    public String toString() {
        return "MessageHistory [token=" + token + ", triggerTime=" + triggerTime + ", startTime=" + startTime + ", endTime="
                + endTime + ", status=" + status + ", stackTrace=" + stackTrace + ", error=" + error + "]";
    }
}
