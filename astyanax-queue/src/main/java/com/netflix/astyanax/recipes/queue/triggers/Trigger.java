package com.netflix.astyanax.recipes.queue.triggers;

/**
 * Base interface for all triggers.  Triggers specify the scheduling for a task in the scheduler.
 * Different implementation of trigger can specify different triggering semantics.  
 * 
 * @author elandau
 *
 */
public interface Trigger {
    /**
     * Process the current trigger and give the next trigger to insert into the queue
     * 
     * @param trigger Current trigger
     * @return  New trigger or null to stop executing the trigger
     */
    Trigger nextTrigger();
    
    /**
     * Get the current trigger time for this trigger.  This is the time
     * for the next execution of the Task
     * @return
     */
    long getTriggerTime();
}
