package com.netflix.astyanax.recipes.scheduler.triggers;

import java.util.concurrent.TimeUnit;

import com.netflix.astyanax.recipes.scheduler.Trigger;

public class RepeatingTrigger extends AbstractTrigger {
    public static class Builder {
        private RepeatingTrigger trigger = new RepeatingTrigger();
        
        public Builder withInterval(long interval, TimeUnit units) {
            trigger.interval = TimeUnit.MILLISECONDS.convert(interval, units);
            return this;
        }
        
        public Builder withDelay(long delay, TimeUnit units) {
            trigger.delay = TimeUnit.MILLISECONDS.convert(delay,  units);
            return this;
        }
        
        public Builder withRepeatCount(long repeatCount) {
            trigger.repeatCount = repeatCount;
            return this;
        }
        
        public Builder withEndTime(long endTime, TimeUnit units) {
            trigger.endTime = TimeUnit.MILLISECONDS.convert(endTime, units);
            return this;
        }
        
        public RepeatingTrigger build() {
            trigger.setTriggerTime(System.currentTimeMillis() + trigger.delay);
            return trigger;
        }
    }

    private long delay          = 0;   // In millseconds
    private long interval       = 0;   // In milliseconds
    private long repeatCount    = 0;   // Repeat count
    private long endTime        = 0;
    
    @Override
    public Trigger nextTrigger() {
        if (getExecutionCount() >= repeatCount) {
            return null;
        }
        
        long currentTime = System.currentTimeMillis();
        long nextTime    = getTriggerTime() + interval;
        if (endTime != 0 && (nextTime > endTime || currentTime > endTime))
            return null;
        
        RepeatingTrigger next = new RepeatingTrigger();
        next.delay        = delay;
        next.interval     = interval;
        next.repeatCount  = repeatCount;
        next.setExecutionCount(getExecutionCount() + 1);
        
        // TODO: Handle missed or delayed execution
        next.setTriggerTime(getTriggerTime() + interval);

        return next;
    }

    public long getDelay() {
        return delay;
    }

    public long getInterval() {
        return interval;
    }

    public long getRepeatCount() {
        return repeatCount;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    public void setInterval(long interval) {
        this.interval = interval;
    }

    public void setRepeatCount(long repeatCount) {
        this.repeatCount = repeatCount;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    @Override
    public String toString() {
        return "RepeatingTrigger [delay=" + delay + ", interval=" + interval + ", repeatCount=" + repeatCount + ", endTime=" + endTime + "] " + super.toString();
    }
}
