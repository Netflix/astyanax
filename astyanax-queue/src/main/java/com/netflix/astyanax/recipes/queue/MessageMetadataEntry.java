/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
