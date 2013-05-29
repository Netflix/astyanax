package com.netflix.astyanax.recipes.queue.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import com.netflix.astyanax.entitystore.TTL;
import com.netflix.astyanax.util.TimeUUIDUtils;

/**
 * Metadata item for a message.  See MessageMetadataType for different types of metadata.
 * 
 * @author elandau
 */
@Entity
public class MessageMetadataEntry {
    @Id
    private String key;
    
    /**
     * Type of column.  See MessageMetadataType
     */
    @Column
    private Byte type;
    
    @Column
    private String name;
    
    @Column
    private String value;
    
    @TTL
    private int ttl;
    
    public MessageMetadataEntry() {
        
    }

    public MessageMetadataEntry(String key, MessageMetadataEntryType type, String name, String value, int ttl) {
        this(key, (byte)type.ordinal(), name, value, ttl);
    }

    public MessageMetadataEntry(String key, byte type, String name, String value, int ttl) {
        this.key   = key;
        this.type  = type;
        this.name  = name;
        this.value = value;
        this.ttl   = ttl;
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
    
    public static MessageMetadataEntry newMessageId(String key, String messageId, int ttl) {
        return new MessageMetadataEntry(key, MessageMetadataEntryType.MessageId, messageId, null, ttl);
    }

    public static MessageMetadataEntry newField(String key, String name, String value, int ttl) {
        return new MessageMetadataEntry(key, MessageMetadataEntryType.Field, name, value, ttl);
    }
    
    public static MessageMetadataEntry newUnique(String key, int ttl) {
        return new MessageMetadataEntry(key, MessageMetadataEntryType.Unique, TimeUUIDUtils.getUniqueTimeUUIDinMicros().toString(), null, ttl);
    }

    public static MessageMetadataEntry newLock(String key, int ttl) {
        return new MessageMetadataEntry(key, MessageMetadataEntryType.Lock,   TimeUUIDUtils.getUniqueTimeUUIDinMicros().toString(), null, ttl);
    }

    public MessageMetadataEntry duplicateForKey(String key) {
        return new MessageMetadataEntry(key, this.type, this.name, this.value, this.ttl);
    }
    
    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }
    
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
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
        return "MessageMetadataEntry [key=" + key + ", type=" + type
                + ", name=" + name + ", value=" + value + ", ttl=" + ttl + "]";
    }
    

}
