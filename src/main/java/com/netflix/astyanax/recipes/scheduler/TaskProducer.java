package com.netflix.astyanax.recipes.scheduler;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

public interface TaskProducer {
    /**
     * Schedule a job for execution
     * @param task
     * @return UUID assigned to this task 
     * 
     * @throws SchedulerException
     */
    UUID scheduleTask(Task task) throws SchedulerException;

    /**
     * Schedule a batch of jobs
     * @param task
     * @return Map of Tasks to their assigned UUIDs
     * 
     * @throws SchedulerException
     */
    Map<Task, UUID> scheduleTasks(Collection<Task> task) throws SchedulerException;
}
