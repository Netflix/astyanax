package com.netflix.astyanax.recipes.queue.triggers;

import com.netflix.astyanax.recipes.scheduler.Trigger;

public abstract class AbstractTrigger implements Trigger {
    private long triggerTime    = 0;   // In milliseconds
    private long executeCount   = 0;
    
    @Override
    public long getTriggerTime() {
        return triggerTime;
    }
    
    public long getExecutionCount() {
        return executeCount;
    }
    
    public void setTriggerTime(long triggerTime) {
        this.triggerTime = triggerTime;
    }
    
    public void setExecutionCount(long executeCount) {
        this.executeCount = executeCount;
    }

    @Override
    public String toString() {
        return "AbstractTrigger [triggerTime=" + triggerTime + ", executeCount=" + executeCount + "]";
    }
}
