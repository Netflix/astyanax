package com.netflix.astyanax.recipes.queue;

import com.netflix.astyanax.annotations.Component;

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

    @Override
    public String toString() {
        return "MessageMetadata [type=" + type + ", name=" + name + "]";
    }
}
