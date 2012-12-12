package com.netflix.astyanax.recipes.scheduler;

/**
 * All scheduler tasks must implement this interface.  An instance of the concrete
 * task class will be created when each trigger is fired and the execute() method
 * will be called with the task context.
 * 
 * @author elandau
 */
@Deprecated
public interface Task {
    /**
     * Called every time the trigger is fired
     * @param task
     */
    void execute(TaskInfo task);
}
