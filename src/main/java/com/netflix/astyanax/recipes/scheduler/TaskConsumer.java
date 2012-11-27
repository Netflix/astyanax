package com.netflix.astyanax.recipes.scheduler;

import java.util.Collection;

import com.netflix.astyanax.recipes.locks.BusyLockException;

public interface TaskConsumer {
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
    
}
