package com.netflix.astyanax.recipes.scheduler;

import java.util.UUID;

import com.netflix.astyanax.annotations.Component;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class SchedulerEntry {
    public SchedulerEntry() {
        
    }
    
    public SchedulerEntry(SchedulerEntryType type, UUID timestamp, SchedulerEntryState state, String instance) {
        super();
        this.type       = (short)type.ordinal();
        this.timestamp  = timestamp;
        this.state      = (short)state.ordinal();
        this.instance   = instance;
    }
    
    /**
     * Type of column.  
     * 0 - Lock
     * 1 - Queue item
     */
    @Component(ordinal=0)
    private Short type;
    
    /**
     * Time when item is to be processed
     */
    @Component(ordinal=1)
    private UUID timestamp;
    
    /**
     * 
     */
    @Component(ordinal=2)
    private Short state;
    
    /**
     * 
     */
    @Component(ordinal=3)
    private String instance;

    public SchedulerEntryType getType() {
        return SchedulerEntryType.values()[type];
    }

    public UUID getTimestamp() {
        return timestamp;
    }

    public SchedulerEntryState getState() {
        return SchedulerEntryState.values()[state];
    }

    public String getInstance() {
        return instance;
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

    public void setInstance(String instance) {
        this.instance = instance;
    }

    @Override
    public String toString() {
        return "SchedulerEntry [type=" + getType() 
                + ", timestamp=" + TimeUUIDUtils.getMicrosTimeFromUUID(timestamp) 
                + ", state="     + getState() 
                + ", instance="  + instance
                + "]";
    }
}