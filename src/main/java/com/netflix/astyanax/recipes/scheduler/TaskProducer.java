package com.netflix.astyanax.recipes.scheduler;

import java.util.UUID;

public interface TaskProducer {
    /**
     * Schedule a job with the provided data
     * @param task
     * @return UUID assigned to this task.  This UUID 
     * 
     * @throws SchedulerException
     */
    UUID scheduleTask(Task task) throws SchedulerException;

}
