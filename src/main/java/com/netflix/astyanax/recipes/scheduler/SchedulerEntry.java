package com.netflix.astyanax.recipes.scheduler;

import java.util.UUID;

import com.netflix.astyanax.annotations.Component;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class SchedulerEntry {
    public SchedulerEntry() {
        
    }
    
    public SchedulerEntry(SchedulerEntryType type, short priority, UUID timestamp, SchedulerEntryState state) {
        super();
        this.type       = (short)type.ordinal();
        this.priority   = 0;
        this.timestamp  = timestamp;
        this.state      = (short)state.ordinal();
    }
    
    /**
     * Type of column.  
     * 0 - Lock
     * 1 - Queue item
     */
    @Component(ordinal=0)
    private Short type;
    
    @Component(ordinal=1)
    private Short priority;
    
    /**
     * Time when item is to be processed
     */
    @Component(ordinal=2)
    private UUID timestamp;
    
    /**
     * 
     */
    @Component(ordinal=3)
    private Short state;
    
    public SchedulerEntryType getType() {
        return SchedulerEntryType.values()[type];
    }

    public UUID getTimestamp() {
        return timestamp;
    }

    public SchedulerEntryState getState() {
        return SchedulerEntryState.values()[state];
    }

    public short getPriority() {
        return priority;
    }
    
    public void setType(Short type) {
        this.type = type;
    }

    public void setTimestamp(UUID timestamp) {
        this.timestamp = timestamp;
    }

    public void setState(Short state) {
        this.state = state;
    }

    public void setPriorty(Short priority) {
        this.priority = priority;
    }

    @Override
    public String toString() {
        return "SchedulerEntry [type=" + getType() 
                + ", timestamp=" + TimeUUIDUtils.getMicrosTimeFromUUID(timestamp) 
                + ", state="     + getState() 
                + "]";
    }
}