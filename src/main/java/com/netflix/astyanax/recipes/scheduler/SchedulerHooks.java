package com.netflix.astyanax.recipes.scheduler;

import java.util.Collection;

import com.netflix.astyanax.MutationBatch;

/**
 * This interface provides a hook to piggyback on top of the executed mutation
 * for each stage of processing
 * 
 * @author elandau
 *
 */
public interface SchedulerHooks {
    /**
     * Called after tasks are read from the queue and before the mutation
     * for updating their state is committed.
     * 
     * @param tasks
     * @param mb
     */
    void preAcquireTasks(Collection<Task> tasks, MutationBatch mb);

    /**
     * Called before a task is released from the queue
     * 
     * @param task
     * @param mb
     */
    void preAckTask(Task task, MutationBatch mb);

    /**
     * Called before a task is inserted in the scheduler
     * @param task
     * @param mb
     */
    void preScheduleTask(Task task, MutationBatch mb);
}
