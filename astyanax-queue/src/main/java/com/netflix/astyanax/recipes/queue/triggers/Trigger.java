package com.netflix.astyanax.recipes.queue.triggers;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 * Base interface for all triggers.  Triggers specify the scheduling for a task in the scheduler.
 * Different implementation of trigger can specify different triggering semantics.  
 * 
 * @author elandau
 *
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, include=JsonTypeInfo.As.PROPERTY, property="@class")
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
     * @return Next execution time in milliseconds (TODO: confirm units)
     */
    long getTriggerTime();
    
    /**
     * @return Return true if this is a trigger for a recurring message.  When true the message will 
     *         perform additional dedup operation
     */
    @JsonIgnore
    boolean isRepeatingTrigger();
}
