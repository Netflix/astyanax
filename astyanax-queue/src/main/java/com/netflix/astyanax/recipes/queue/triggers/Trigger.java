/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
