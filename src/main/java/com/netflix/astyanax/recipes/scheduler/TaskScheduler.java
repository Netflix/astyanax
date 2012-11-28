package com.netflix.astyanax.recipes.scheduler;

import java.util.Map;

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
     * Return the number of tasks in the queue.  This is an estimate.
     * This is an expensive operation and should be used sparingly.
     * @return  Number of tasks, including tasks currently being processed
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

    /**
     * Get the counts for each shard in the scheduler.  This is an estimate.
     * This is an expensive operation and should be used sparingly.
     * @return
     * @throws SchedulerException
     */
    Map<String, Integer> getShardCounts() throws SchedulerException;
    
    /**
     * Create a consumer of the task scheduler.  The consumer will have it's own context
     * 
     * @return
     * @throws SchedulerException
     */
    TaskConsumer createConsumer();

    /**
     * Create a producer of tasks for this scheduler.
     * @return
     * @throws SchedulerException
     */
    TaskProducer createProducer();
}
