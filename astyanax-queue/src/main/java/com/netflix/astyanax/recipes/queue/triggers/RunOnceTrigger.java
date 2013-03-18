package com.netflix.astyanax.recipes.queue.triggers;

import java.util.concurrent.TimeUnit;


public class RunOnceTrigger extends AbstractTrigger {
    public static class Builder {
        private RunOnceTrigger trigger = new RunOnceTrigger();
        
        public Builder withDelay(long delay, TimeUnit units) {
            trigger.delay = TimeUnit.MILLISECONDS.convert(delay,  units);
            return this;
        }
        
        public RunOnceTrigger build() {
            if (trigger.delay != null)
                trigger.setTriggerTime(System.currentTimeMillis() + trigger.delay);
            else 
                trigger.setTriggerTime(System.currentTimeMillis());
            return trigger;
        }
    }

    private Long delay;   // In millseconds
    
    @Override
    public Trigger nextTrigger() {
        // There is no next trigger.
        return null;
    }
}
