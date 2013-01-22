package com.netflix.astyanax.recipes.queue;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;

import com.netflix.astyanax.annotations.Component;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class MessageQueueEntry {
    private static final String ID_DELIMITER = ":";
            
    /**
     * Type of column.  
     * 0 - Lock
     * 1 - Queue item
     */
    @Component(ordinal=0)
    private Byte type;
    
    @Component(ordinal=1)
    private Byte priority;
    
    /**
     * Time when item is to be processed
     */
    @Component(ordinal=2)
    private UUID timestamp;
    
    /**
     * 
     */
    @Component(ordinal=3)
    private Byte state;
    
    @Component(ordinal=4)
    private UUID random = TimeUUIDUtils.getUniqueTimeUUIDinMicros();

    public MessageQueueEntry() {
    }
    
    public MessageQueueEntry(String id) {
        String[] parts = StringUtils.split(id, ID_DELIMITER);
        if (parts.length != 5)
            throw new RuntimeException("Invalid message ID.  Expection <type>:<priority>:<timestamp>:<state> but got " + id);
        
        type      = Byte.parseByte(parts[0]);
        priority  = Byte.parseByte(parts[1]);
        timestamp = UUID.fromString (parts[2]);
        state     = Byte.parseByte(parts[3]);
        random    = UUID.fromString (parts[4]);
    }
    
    private MessageQueueEntry(MessageQueueEntryType type, byte priority, UUID timestamp, MessageQueueEntryState state) {
        super();
        this.type       = (byte)type.ordinal();
        this.priority   = 0;
        this.timestamp  = timestamp;
        this.state      = (byte)state.ordinal();
    }
    
    public static MessageQueueEntry newLockEntry(MessageQueueEntryState state) {
        return new MessageQueueEntry(MessageQueueEntryType.Lock, (byte)0, TimeUUIDUtils.getUniqueTimeUUIDinMicros(), state);
    }
    
    public static MessageQueueEntry newLockEntry(UUID timestamp, MessageQueueEntryState state) {
        return new MessageQueueEntry(MessageQueueEntryType.Lock, (byte)0, timestamp, state);
    }
    
    public static MessageQueueEntry newMetadataEntry() {
        return new MessageQueueEntry(MessageQueueEntryType.Metadata, (byte)0, null, MessageQueueEntryState.None);
    }
    
    public static MessageQueueEntry newMessageEntry(byte priority, UUID timestamp, MessageQueueEntryState state) {
        return new MessageQueueEntry(MessageQueueEntryType.Message,  priority, timestamp, state);
    }
    
    public MessageQueueEntryType getType() {
        return MessageQueueEntryType.values()[type];
    }

    public UUID getTimestamp() {
        return timestamp;
    }
    
    public long getTimetsamp(TimeUnit units) {
        return units.convert(TimeUUIDUtils.getMicrosTimeFromUUID(timestamp), TimeUnit.MICROSECONDS);
    }

    public MessageQueueEntryState getState() {
        return MessageQueueEntryState.values()[state];
    }

    public byte getPriority() {
        return priority;
    }
    
    public void setType(Byte type) {
        this.type = type;
    }

    public void setTimestamp(UUID timestamp) {
        this.timestamp = timestamp;
    }

    public void setState(Byte state) {
        this.state = state;
    }

    public void setPriorty(Byte priority) {
        this.priority = priority;
    }

    public String getMessageId() {
        return new StringBuilder()
                .append(type)                .append(ID_DELIMITER)
                .append(priority)            .append(ID_DELIMITER)
                .append(timestamp.toString()).append(ID_DELIMITER)
                .append(state)               .append(ID_DELIMITER)
                .append(random.toString())
                .toString();
        
    }
    
    @Override
    public String toString() {
        return "MessageQueueEntry [" + getType() + " " + priority + " " + timestamp + " " + getState() + "]";
    }
}