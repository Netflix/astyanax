package com.netflix.astyanax.recipes.queue.entity;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OrderBy;

import org.apache.commons.lang.StringUtils;

import com.netflix.astyanax.entitystore.TTL;
import com.netflix.astyanax.recipes.queue.Message;
import com.netflix.astyanax.recipes.queue.MessageQueueUtils;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.TimeUUIDUtils;

/**
 * Entry in the ordered message queue.
 * 
 * @author elandau
 */
@Entity
public class MessageQueueEntry {
    private static final String ID_DELIMITER    = ":";
    private static final String SHARD_DELIMITER = "$";
            
    @Id
    private String shardName;

    /**
     * Type of column.  
     * 0 - Lock
     * 1 - Queue item
     */
    @Column
    private Byte type;
    
    /**
     * Up to 256 priority levels.  Only priority 0 is used for delayed execution.
     * Other priorities have the semantics of being executed immediately in priority order
     * with events within each priority being processed in order of their timestamp.  For 
     * high priority messages timestamp is used to provide additional ordering and will
     * not be used for delayed execution.
     */
    @Column
    @OrderBy("DESC")
    private Byte priority;
    
    /**
     * Time when item is to be processed
     */
    @Column
    private UUID timestamp;
    
    /**
     * Random number to help ensure each entry is unique
     */
    @Column
    private UUID random;

    /**
     * 
     */
    @Column
    private Byte state;
    
    @Column
    private ByteBuffer body;
    
    @TTL
    private int ttl = 0;
    
    public MessageQueueEntry() {
    }
    
    public MessageQueueEntry(String id) {
        String[] segments = StringUtils.split(id, SHARD_DELIMITER);
        if (segments.length == 2) {
            shardName = segments[0];
            id = segments[1];
        }
        
        String[] parts = StringUtils.split(id, ID_DELIMITER);
        if (parts.length != 5)
            throw new RuntimeException("Invalid message ID.  Expected format <type>:<priority>:<timestamp>:<random>:<state> but got " + id);
        
        type      = Byte.parseByte(parts[0]);
        priority  = Byte.parseByte(parts[1]);
        timestamp = UUID.fromString (parts[2]);
        random    = UUID.fromString (parts[3]);
        state     = Byte.parseByte(parts[4]);
    }
    
    private MessageQueueEntry(String shardName, MessageQueueEntryType type, byte priority, long timestamp, UUID random, MessageQueueEntryState state, String body, int ttl) {
        super();
        
        this.shardName  = shardName;
        this.type       = (byte)type.ordinal();
        this.state      = (byte)state.ordinal();
        this.priority   = priority;
        this.body       = StringSerializer.get().toByteBuffer(body);
        this.ttl        = ttl;

        if (timestamp == 0L)
            this.timestamp  = TimeUUIDUtils.getUniqueTimeUUIDinMicros();
        else
            this.timestamp  = TimeUUIDUtils.getMicrosTimeUUID(timestamp);
        
        if (random == null)
            this.random     = TimeUUIDUtils.getUniqueTimeUUIDinMicros();
        else 
            this.random     = random;
    }
    
    public static MessageQueueEntry newLockEntry(String shardName, MessageQueueEntryState state, int ttl) {
        return new MessageQueueEntry(
                shardName, 
                MessageQueueEntryType.Lock, 
                (byte)0, 
                0, 
                null, 
                state,
                null,
                ttl);
    }
    
    /**
     * 
     * @param shardName
     * @param priority
     * @param timestamp     Timestamp in MILISECONDS
     * @param state
     * @param body
     * @param ttl
     * @return
     */
    public static MessageQueueEntry newMessageEntry(String shardName, byte priority, long timestamp, MessageQueueEntryState state, String body, Integer ttl) {
        return new MessageQueueEntry(
                shardName, 
                MessageQueueEntryType.Message, 
                priority, 
                TimeUnit.MICROSECONDS.convert(timestamp, TimeUnit.MILLISECONDS), 
                null, 
                state, 
                body, 
                ttl);
    }
    
    public static MessageQueueEntry newBusyEntry(String shardName, Message message, MessageQueueEntry previous, int ttl) {
        return new MessageQueueEntry(
                shardName,
                MessageQueueEntryType.Message, 
                (byte)message.getPriority(), 
                TimeUnit.MICROSECONDS.convert(message.getTriggerTime() + message.getTimeout(), TimeUnit.MILLISECONDS),   // TODO: double check units
                null, 
                MessageQueueEntryState.Busy, 
                previous.getBodyAsString(), 
                ttl);
    }
    
    public static MessageQueueEntry fromMetadata(MessageMetadataEntry meta) {
        String parts[] = StringUtils.split(meta.getName(), "$");
        return new MessageQueueEntry(parts[1]);
    }

    public MessageQueueEntryType getType() {
        return MessageQueueEntryType.values()[type];
    }

    public UUID getTimestamp() {
        return timestamp;
    }
    
    public long getTimestamp(TimeUnit units) {
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

    public void setState(MessageQueueEntryState state) {
        this.state = (byte)state.ordinal();
    }

    public void setPriorty(Byte priority) {
        this.priority = priority;
    }

    public String getMessageId() {
        return StringUtils.join(
                new String[]{
                        type.toString(), 
                        priority.toString(), 
                        timestamp.toString(), 
                        random.toString(), 
                        state.toString()}, 
                ID_DELIMITER);
        
    }

    public String getFullMessageId() {
        return shardName + SHARD_DELIMITER + StringUtils.join(
                new String[]{
                        type.toString(), 
                        priority.toString(), 
                        timestamp.toString(), 
                        random.toString(), 
                        state.toString()}, 
                ID_DELIMITER);
        
    }

    public UUID getRandom() {
        return random;
    }

    public void setRandom(UUID random) {
        this.random = random;
    }
    
    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }
    
    public String getBodyAsString() {
        return StringSerializer.get().fromByteBuffer(body);
    }
    
    public Message getBodyAsMessage() throws Exception {
        return MessageQueueUtils.deserializeByteBuffer(body, Message.class);
    }

    public void setBodyFromString(String body) {
        this.body = StringSerializer.get().toByteBuffer(body);
    }
    
    public Long getBodyAsLong() {
        return LongSerializer.get().fromByteBuffer(body);
    }
    
    public void setBodyFromLong(Long body) {
        this.body = LongSerializer.get().toByteBuffer(body);
    }

    public String getShardName() {
        return shardName;
    }
    
    public void setShardName(String shardName) {
        this.shardName = shardName;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MessageQueueEntry [");
        sb.append(  "shard="   ).append(shardName);
        sb.append(", type="    ).append(MessageQueueEntryType.values()[type]);
        sb.append(", priority=").append(priority);
        if (timestamp != null)
            sb.append(", timestamp=").append(timestamp)
              .append("(").append(TimeUUIDUtils.getTimeFromUUID(this.timestamp)).append(")");
        sb.append(", random="  ).append(random);
        sb.append(", state="   ).append(MessageQueueEntryState.values()[state]);
        sb.append(", ttl="     ).append(ttl);
        if (this.body == null) {
            sb.append(", body=null");
        }
        else {
            switch (getType()) {
            case Lock:
                sb.append(", body=").append(this.getBodyAsLong());
                break;
            case Message:
                sb.append(", msglength=").append(this.body.limit());
            }
        }
        sb.append("]");
        return sb.toString();
    }

}