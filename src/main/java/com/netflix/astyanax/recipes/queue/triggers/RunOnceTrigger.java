package com.netflix.astyanax.recipes.queue.triggers;

import com.netflix.astyanax.recipes.scheduler.Trigger;

public class RunOnceTrigger implements Trigger {
    @Override
    public Trigger nextTrigger() {
        return null;
    }

    @Override
    public long getTriggerTime() {
        return 0;
    }
}
