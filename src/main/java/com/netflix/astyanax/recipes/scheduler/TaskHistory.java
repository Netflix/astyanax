package com.netflix.astyanax.recipes.scheduler;

/**
 * Track history for a single execution of a task
 * 
 * @author elandau
 *
 */
public class TaskHistory {
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
    private TaskStatus  status;
    
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
    public TaskStatus getStatus() {
        return status;
    }
    public void setStatus(TaskStatus status) {
        this.status = status;
    }
    public String getStackTrace() {
        return stackTrace;
    }
    public void setStackTrace(String stackTrace) {
        this.stackTrace = stackTrace;
    }
    public String getException() {
        return error;
    }
    public void setError(String exception) {
        this.error = exception;
    }
}
