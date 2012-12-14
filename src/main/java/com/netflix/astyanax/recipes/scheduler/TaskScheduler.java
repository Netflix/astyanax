package com.netflix.astyanax.recipes.scheduler;

import java.util.Collection;

import com.netflix.astyanax.recipes.queue.triggers.Trigger;
import com.netflix.astyanax.recipes.uniqueness.NotUniqueException;

/**
 * Common interface for a task scheduler
 */
@Deprecated
public interface TaskScheduler {

    /**
     * Create the underlying storage for the scheduler
     * @throws TaskSchedulerException
     */
    void create() throws TaskSchedulerException;

    /**
     * Start the scheduler processing threads
     * @throws TaskSchedulerException
     */
    void start() throws TaskSchedulerException;

    /**
     * Stop the scheduler processing threads
     * @return True if stopped successfully before timing out of false otherwise
     * @throws TaskSchedulerException
     */
    boolean stop() throws TaskSchedulerException;

    /**
     * Scheduler a task to run using the provided trigger
     * @param task - Information about how to run the task
     * @param trigger - Can be null for single execution
     * @throws TaskSchedulerException
     * @throws NotUniqueException 
     */
    void scheduleTask(TaskInfo task, Trigger trigger) throws TaskSchedulerException, NotUniqueException;

    /**
     * Stop a running task
     * @param taskKey
     * @throws TaskSchedulerException
     */
    void stopTask(String taskKey) throws TaskSchedulerException;

    /**
     * Start a stopped task
     * @param taskKey
     * @throws TaskSchedulerException
     */
    void startTask(String taskKey) throws TaskSchedulerException;

    /**
     * Delete a task
     * 
     * @param taskKey
     * @throws TaskSchedulerException
     */
    void deleteTask(String taskKey) throws TaskSchedulerException;

    /**
     * Get the execution history for a task 
     * @param taskKey   - Unique task id
     * @param timeFrom  - History from this time
     * @param timeTo    - History to this time
     * @return Collection of history elements
     * @throws TaskSchedulerException
     */
    Collection<TaskHistory> getTaskHistory(String taskKey, Long timeFrom, Long timeTo, int count) throws TaskSchedulerException;

}