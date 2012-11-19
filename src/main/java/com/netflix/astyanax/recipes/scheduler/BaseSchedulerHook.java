package com.netflix.astyanax.recipes.scheduler;

import java.util.Collection;

import com.netflix.astyanax.MutationBatch;

public class BaseSchedulerHook implements SchedulerHooks {
    @Override
    public void preAcquireTasks(Collection<Task> tasks, MutationBatch mb) {
    }

    @Override
    public void preAckTask(Task task, MutationBatch mb) {
    }

    @Override
    public void preScheduleTask(Task task, MutationBatch mb) {
    }
}
