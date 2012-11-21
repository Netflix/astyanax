package com.netflix.astyanax.recipes.scheduler;

import java.util.Collection;
import java.util.UUID;

import com.netflix.astyanax.recipes.locks.BusyLockException;

/**
 * Base interface for a distributed task scheduler.
 * 
 * Common use pattern
 * 
 *  TaskScheduler scheduler = ...;
 *  Collection<Task> tasks = scheduler.acquireTasks(10);
 *  for (Task task : tasks) {
 *      try {
 *          // Do something with this task
 *      }
 *      finally {
 *          scheduler.ackTask(task);
 *      }
 *  }
 *  
 * @author elandau
 *
 */
public interface TaskScheduler {
    /**
     * Schedule a job with the provided data
     * @param task
     * @return UUID assigned to this task.  This UUID 
     * 
     * @throws SchedulerException
     */
    UUID scheduleTask(Task task) throws SchedulerException;
    
    /**
     * Acquire N items from the queue.  Each item must be released
     * 
     * TODO: Items since last process time
     * 
     * @param itemsToPop
     * @return
     * @throws InterruptedException 
     * @throws Exception 
     */
    Collection<Task> acquireTasks(int itemsToPop) throws SchedulerException, BusyLockException, InterruptedException;

    /**
     * Release a job after completion
     * @param item
     * @throws Exception 
     */
    void ackTask(Task task) throws SchedulerException;

    /**
     * Release a set of jobs
     * @param items
     */
    void ackTasks(Collection<Task> tasks) throws SchedulerException;
    
    /**
     * Return the number of tasks in the queue
     * @return
     */
    long getTaskCount() throws SchedulerException;
    
    /**
     * Clear all tasks in the queue
     * @throws SchedulerException
     */
    void clearTasks() throws SchedulerException;
    
    /**
     * Create the underlying storage for this scheduler
     * 
     * @throws SchedulerException
     */
    void createScheduler() throws SchedulerException;
}
