package com.netflix.astyanax.recipes.queue;

import com.netflix.astyanax.annotations.Component;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class MessageMetadataEntry {
    /**
     * Type of column.  See MessageMetadataType
     */
    @Component(ordinal=0)
    private Byte type;
    
    @Component(ordinal=1)
    private String name;
    
    public MessageMetadataEntry() {
        
    }

    public MessageMetadataEntry(MessageMetadataEntryType type, String name) {
        this.type = (byte)type.ordinal();
        this.name = name;
    }

    public Byte getType() {
        return type;
    }

    public MessageMetadataEntryType getMetadataType() {
        return MessageMetadataEntryType.values()[type];
    }
    public void setType(Byte type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    
    public static MessageMetadataEntry newMessageId(String messageId) {
        return new MessageMetadataEntry(MessageMetadataEntryType.MessageId, messageId);
    }

    public static MessageMetadataEntry newField(String name) {
        return new MessageMetadataEntry(MessageMetadataEntryType.Field, name);
    }
    
    public static MessageMetadataEntry newUnique() {
        return new MessageMetadataEntry(MessageMetadataEntryType.Unique, TimeUUIDUtils.getUniqueTimeUUIDinMicros().toString());
    }

    public static MessageMetadataEntry newLock() {
        return new MessageMetadataEntry(MessageMetadataEntryType.Lock,   TimeUUIDUtils.getUniqueTimeUUIDinMicros().toString());
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MessageMetadataEntry other = (MessageMetadataEntry) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (type == null) {
            if (other.type != null)
                return false;
        } else if (!type.equals(other.type))
            return false;
        return true;
    }
    
    @Override
    public String toString() {
        return "MessageMetadata [type=" + MessageMetadataEntryType.values()[type] + ", name=" + name + "]";
    }
}
