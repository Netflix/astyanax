package com.netflix.astyanax.recipes.queue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Equality;
import com.netflix.astyanax.recipes.locks.BusyLockException;
import com.netflix.astyanax.recipes.scheduler.Trigger;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.retry.RunOnce;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;
import com.netflix.astyanax.util.TimeUUIDUtils;

/**
 * ShardedDistributedMessageQueue is a Cassandra backed client driven message queue.  
 * 
 * Key features
 * 1.  Time partition circular row key set used to time bound how much a wide row can grow.  This,
 *      along with an aggressive gc_grace_period will give cassandra a chance to clear out the row
 *      before the clients cycle back to the time partition.  Only one partition is active at any
 *      given time.
 * 2.  Mod sharding per partition based on message time.  This solves the problem of lock contention 
 *      on the acitve time partition.
 * 3.  Smart processing of partitions and shards to read mostly from the current time shard but allowing
 *      some cycle for processing older shards
 * 4.  Read-ack model of removing elements from the queue.  As part of removing an element from the queue
 *      the client inserts a timeout message.  Once the message has been processed the timeout message is removed 
 *      from the queue.  Otherwise it will be processed if it's time arrived and it is still in the queue.
 * 5.  Batch read of events
 * 6.  Batch insert of events
 * 
 * Algorithm:
 * 
 *  Messages are stored as columns in an index where the columns are stored in time order.  The time can
 *  be the current time for immediate execution or future time for recurring or scheduled messages.
 *  Jobs will be processed in time order.
 *  
 *  To achieve higher scalability the job queue (implemented as a row) is sharded by a user provided shard.
 *  Rows also implement a rolling time window which is used to alleviate tombstone pressure 
 * 
 * Enque:
 *  
 * Deque:
 *  1.  Lock + read top N columns
 *  2.  Select M jobs to process
 *      Select jobs in <state> = scheduled
 *      If any jobs are marked as processing then delete and update their state
 *  3.  Release the lock with a mutation that has a 
 *      delete for the columns being processed and 
 *      insert with the same data but <state> = processing
 *  4.  Process the jobs
 *  5.  If the processing thread is about to enter a section which is not repeatable then update the column 
 *      by changing the state to NotRepeatable.
 *  6.  Issue a delete for processed job
 *  
 * Schema:
 *      RowKey: TimeBucket + Shard
 *      Column: <type><priority><timeuuid><state>
 *      Value:  Job Data
 *      
 *      <type>
 *          0 - Lock meta
 *          1 - Queue item
 *      <state>
 *          0 - Lock columns - There are special columns that are used to lock the row
 *          1 - Scheduled
 *          2 - Processing - timeuuid = timeout
 *          3 - NotRepeatable - special indicator that tells the queue that the job is not replayble since there could
 *                          be a persistence 
 *          
 *    
 * Recurring Messages:
 * 
 * @author elandau
 *
 */
public class ShardedDistributedMessageQueue implements MessageQueue {
    private static final Logger LOG = LoggerFactory.getLogger(ShardedDistributedMessageQueue.class);
    
    public static final String           DEFAULT_COLUMN_FAMILY_NAME      = "Queues";
    public static final String           DEFAULT_QUEUE_NAME              = "Queue";
    public static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL       = ConsistencyLevel.CL_LOCAL_QUORUM;
    public static final RetryPolicy      DEFAULT_RETRY_POLICY            = RunOnce.get();
    public static final long             DEFAULT_LOCK_TIMEOUT            = TimeUnit.MICROSECONDS.convert(30,  TimeUnit.SECONDS);
    public static final long             DEFAULT_LOCK_TTL                = TimeUnit.MICROSECONDS.convert(2,   TimeUnit.MINUTES);
    public static final long             DEFAULT_POLL_WAIT               = TimeUnit.MILLISECONDS.convert(100, TimeUnit.MILLISECONDS);
    public static final Boolean          DEFAULT_POISON_QUEUE_ENABLED    = false;
    public static final String           KEY_INDEX_SUFFIX                = "_KEY_INDEX";
    
    private final static AnnotatedCompositeSerializer<MessageQueueEntry> entrySerializer     
        = new AnnotatedCompositeSerializer<MessageQueueEntry>(MessageQueueEntry.class);
    private final static AnnotatedCompositeSerializer<MessageMetadataEntry>   metadataSerializer  
        = new AnnotatedCompositeSerializer<MessageMetadataEntry>(MessageMetadataEntry.class);
    
    private static final ObjectMapper mapper = new ObjectMapper();
    
    {
        mapper.getSerializationConfig().setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
        mapper.enableDefaultTyping();
    }

    /**
     * 
     * @author elandau
     */
    public static class Builder {
        private ShardedDistributedMessageQueue queue = new ShardedDistributedMessageQueue();
        private String columnFamilyName              = DEFAULT_COLUMN_FAMILY_NAME;

        public Builder withColumnFamily(String columnFamilyName) {
            this.columnFamilyName = columnFamilyName;
            return this;
        }
        
        public Builder withShardCount(int count) {
            queue.settings.setShardCount(count);
            return this;
        }
        
        public Builder withBuckets(int bucketCount, int bucketDuration, TimeUnit units) {
            queue.settings.setPartitionDuration(TimeUnit.MICROSECONDS.convert(bucketDuration,  units));
            queue.settings.setPartitionCount(bucketCount);
            return this;
        }
        
        public Builder withRetentionTimeout(Long timeout, TimeUnit units) {
            queue.settings.setRetentionTimeout(timeout,  units);
            return this;
        }
        
        public Builder withLockTimeout(Long timeout, TimeUnit units) {
            queue.lockTimeout = TimeUnit.MICROSECONDS.convert(timeout,  units);
            return this;
        }
        
        public Builder withLockTtl(Long ttl, TimeUnit units) {
            queue.lockTtl = TimeUnit.SECONDS.convert(ttl,  units);
            return this;
        }
        
        public Builder withPollInterval(Long internval, TimeUnit units) {
            queue.pollInterval = TimeUnit.MILLISECONDS.convert(internval,  units);
            return this;
        }
        
        public Builder withQueueName(String queueName) {
            queue.queueName = queueName;
            return this;
        }
        
        public Builder withConsistencyLevel(ConsistencyLevel level) {
            queue.consistencyLevel = level;
            return this;
        }
        
        public Builder withKeyspace(Keyspace keyspace) {
            queue.keyspace = keyspace;
            return this;
        }
        
        public Builder withStats(MessageQueueStats stats) {
            queue.stats = stats;
            return this;
        }
        
        public Builder withHook(MessageQueueHooks hooks) {
            queue.hooks.add(hooks);
            return this;
        }
        
        public Builder withHooks(Collection<MessageQueueHooks> hooks) {
            queue.hooks.addAll(hooks);
            return this;
        }
        
        public Builder withPoisonQueue(Boolean enabled) {
            queue.bPoisonQueueEnabled = enabled;
            return this;
        }
        
        public ShardedDistributedMessageQueue build() {
            queue.queueColumnFamily         = ColumnFamily.newColumnFamily(columnFamilyName, StringSerializer.get(), entrySerializer); 
            queue.keyIndexColumnFamily = ColumnFamily.newColumnFamily(columnFamilyName + KEY_INDEX_SUFFIX, StringSerializer.get(), metadataSerializer); 
            
            queue.initialize();
            return queue;
        }
    }

    // Immutable after configuration
    private ColumnFamily<String, MessageQueueEntry> queueColumnFamily;
    private ColumnFamily<String, MessageMetadataEntry>   keyIndexColumnFamily;
    
    private Keyspace                        keyspace;
    private ConsistencyLevel                consistencyLevel    = DEFAULT_CONSISTENCY_LEVEL;
    private long                            lockTimeout         = DEFAULT_LOCK_TIMEOUT;
    private Long                            lockTtl             = DEFAULT_LOCK_TTL;
    private long                            pollInterval        = DEFAULT_POLL_WAIT;
    private MessageQueueStats               stats               = new CountingQueueStats();
    private String                          queueName           = DEFAULT_QUEUE_NAME;
    private AtomicLong                      counter             = new AtomicLong(new Random().nextInt(1000));
    private Collection<MessageQueueHooks>   hooks               = Lists.newArrayList();
    private MessageQueueSettings            settings            = new MessageQueueSettings();
    private List<MessageQueueShard>         partitions;
    private Boolean                         bPoisonQueueEnabled = DEFAULT_POISON_QUEUE_ENABLED;
    private Function<String, Message>       invalidMessageHandler  = new Function<String, Message>() {
                                                                        @Override
                                                                        public Message apply(@Nullable String input) {
                                                                            LOG.warn("Invalid message: " + input);
                                                                            return null;
                                                                        }
                                                                    };
    
    private void initialize() {
        Preconditions.checkArgument(
                lockTtl == null || lockTimeout < lockTtl, 
                "Timeout " + lockTtl + " must be less than TTL " + lockTtl);
        Preconditions.checkNotNull(keyspace, "Must specify keyspace");
        
        try {
            Column<MessageQueueEntry> column = keyspace.prepareQuery(queueColumnFamily)
                    .getRow(queueName)
                    .getColumn(MessageQueueEntry.newMetadataEntry())
                    .execute()
                    .getResult();
            
            ByteArrayInputStream bais = new ByteArrayInputStream(column.getByteArrayValue());
            settings = mapper.readValue(bais, MessageQueueSettings.class);
        } 
        catch (Exception e) {
        }
        
        partitions = Lists.newArrayList();
        
        for (int i = 0; i < settings.getPartitionCount(); i++) {
            for (int j = 0; j < settings.getShardCount(); j++) {
                partitions.add(new MessageQueueShard(queueName + ":" + i + ":" + j, i, j));
            }
        }
        
    }
    

    protected MessageQueueEntry getBusyEntry(Message message) {
        return MessageQueueEntry.newMessageEntry(message.getPriority(), message.getToken(), MessageQueueEntryState.Busy);
    }
    
    /**
     * Return the shard for this message
     * @param message
     * @return
     */
    protected String getShardKey(MessageQueueEntry message) {
        return getShardKey(TimeUUIDUtils.getMicrosTimeFromUUID(message.getTimestamp()));
    }
    
    /**
     * Return the shard for this timestamp
     * @param message
     * @return
     */
    private String getShardKey(long messageTime) {
        long timePartition = (messageTime / settings.getPartitionDuration()) % settings.getPartitionCount();
        long shard         =  messageTime % settings.getShardCount();
        return queueName + ":" + timePartition + ":" + shard;
    }
    
    private String getCompositeKey(String name, String key) {
        return name + "$" + key;
    }
    
    private static String[] splitCompositeKey(String key) throws MessageQueueException {
        String[] parts = StringUtils.split(key, "$");
        
        if (parts.length != 2) {
            throw new MessageQueueException("Invalid key.  Expected format <queue|shard>$<name>. " + key);
        }

        return parts;
    }
    
    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public long getMessageCount() throws MessageQueueException {
        Map<String, Integer> counts = getShardCounts();
        long count = 0;
        for (Integer value : counts.values()) {
            count += value;
        }
        return count;
    }
    
    @Override
    public Map<String, Integer> getShardCounts() throws MessageQueueException {
        try {
            List<String> keys = Lists.newArrayList();
            for (int i = 0; i < settings.getPartitionCount(); i++) {
                for (int j = 0; j < settings.getShardCount(); j++) {
                    keys.add(queueName + ":" + i + ":" + j);
                }
            }
            
            Map<String, Integer> result = Maps.newTreeMap();
            result.putAll(keyspace.prepareQuery(queueColumnFamily)
                    .getKeySlice(keys)
                    .getColumnCounts()
                    .execute()
                    .getResult());
            return result;
        } catch (ConnectionException e) {
            throw new MessageQueueException("Failed to get counts", e);
        }
    }

    public String getCurrentPartition() {
        long timePartition = TimeUnit.MICROSECONDS.convert(System.currentTimeMillis(),  TimeUnit.MILLISECONDS)/settings.getPartitionDuration();
        return queueName + ":" + ((timePartition + settings.getPartitionCount()) % settings.getPartitionCount());
    }

    @Override
    public void clearMessages() throws MessageQueueException {
        // TODO:  Clear columns from all shards
    }
    
    @Override
    public Message readMessage(String messageId) throws MessageQueueException {
        String[] parts = splitCompositeKey(messageId);
        
        String shardKey = parts[0];
        MessageQueueEntry entry = new MessageQueueEntry(parts[1]);
        
        try {
            Column<MessageQueueEntry> column = keyspace
                    .prepareQuery(queueColumnFamily)
                    .setConsistencyLevel(consistencyLevel)
                    .getKey(shardKey)
                        .getColumn(entry)
                    .execute().getResult();
            try {
                ByteArrayInputStream bais = new ByteArrayInputStream(column.getByteArrayValue());
                return mapper.readValue(bais, Message.class);
            } catch (Exception e) {
                // Error parsing the message so we pass it on to the invalid message handler.
                try {
                    return invalidMessageHandler.apply(column.getStringValue());
                }
                catch (Exception e2) {
                    throw new MessageQueueException("Error parsing message " + messageId);
                }
            } 
        } catch (NotFoundException e) {
            return null;
        } catch (ConnectionException e) {
            throw new MessageQueueException("Error getting message " + messageId, e);
        }
    }
    
    @Override
    public Message readMessageByKey(String key) throws MessageQueueException {
        String groupRowKey = getCompositeKey(queueName, key);
        try {
            ColumnList<MessageMetadataEntry> columns = keyspace.prepareQuery(keyIndexColumnFamily)
                .getRow(groupRowKey)
                .withColumnRange(metadataSerializer.buildRange()
                    .greaterThanEquals((byte)MessageMetadataEntryType.MessageId.ordinal())
                    .lessThanEquals((byte)MessageMetadataEntryType.MessageId.ordinal())
                    .build()
                )
                .execute()
                .getResult();
            
            for (Column<MessageMetadataEntry> entry : columns) {
                // Return the first one we get.  Hmmm... maybe we want to do some validation checks here
                return readMessage(entry.getName().getName());
            }
            return null;
        } catch (NotFoundException e) {
            return null;
        } catch (ConnectionException e) {
            throw new MessageQueueException("Error fetching row " + groupRowKey, e);
        }
    }

    @Override
    public boolean deleteMessageByKey(String key) throws MessageQueueException {
        MutationBatch mb = keyspace.prepareMutationBatch();
        
        String groupRowKey = getCompositeKey(queueName, key);
        try {
            ColumnList<MessageMetadataEntry> columns = keyspace.prepareQuery(keyIndexColumnFamily)
                .getRow(groupRowKey)
                .withColumnRange(metadataSerializer.buildRange()
                    .greaterThanEquals((byte)MessageMetadataEntryType.MessageId.ordinal())
                    .lessThanEquals((byte)MessageMetadataEntryType.MessageId.ordinal())
                    .build()
                )
                .execute()
                .getResult();
            
            for (Column<MessageMetadataEntry> entry : columns) {
                String[] parts = splitCompositeKey(entry.getName().getName());
                
                String shardKey = parts[0];
                MessageQueueEntry queueEntry = new MessageQueueEntry(parts[1]);
                
                mb.withRow(queueColumnFamily, shardKey).deleteColumn(queueEntry);
            }
            
            mb.withRow(keyIndexColumnFamily, groupRowKey).delete();
        } catch (NotFoundException e) {
            return false;
        } catch (ConnectionException e) {
            throw new MessageQueueException("Error fetching row " + groupRowKey, e);
        }
        
        try {
            mb.execute();
        } catch (ConnectionException e) {
            throw new MessageQueueException("Error deleting queue item " + groupRowKey, e);
        }
        
        return true;
    }

    @Override
    public void deleteMessage(String messageId) throws MessageQueueException {
        String[] parts = splitCompositeKey(messageId);
        
        String shardKey = parts[0];
        MessageQueueEntry entry = new MessageQueueEntry(parts[1]);
        
        try {
            keyspace.prepareColumnMutation(queueColumnFamily, shardKey, entry)
                .setConsistencyLevel(consistencyLevel)
                .deleteColumn().execute();
        }
        catch (ConnectionException e) {
            throw new MessageQueueException("Error deleting message " + messageId, e);
        }
    }
    
    @Override
    public void deleteMessages(Collection<String> messageIds) throws MessageQueueException {
        MutationBatch mb = keyspace.prepareMutationBatch()
            .setConsistencyLevel(consistencyLevel);
        
        for (String messageId : messageIds) {
            String[] parts = splitCompositeKey(messageId);
            String shardKey = parts[0];
            MessageQueueEntry entry = new MessageQueueEntry(parts[1]);

            mb.withRow(queueColumnFamily, shardKey)
                .deleteColumn(entry);
        }
        
        try {
            mb.execute();
        }
        catch (ConnectionException e) {
            throw new MessageQueueException("Error deleting messages " + messageIds, e);
        }
    }

    @Override
    public void createStorage() throws MessageQueueException {
        try {
            keyspace.createColumnFamily(this.queueColumnFamily, ImmutableMap.<String, Object>builder()
                    .put("key_validation_class",     "UTF8Type")
                    .put("comparator_type",          "CompositeType(BytesType, BytesType(reversed=true), TimeUUIDType, BytesType)")
                    .put("read_repair_chance",       1.0)
                    .put("gc_grace_period",          0)     // TODO: Calculate gc_grace_period
                    .build());
        } catch (ConnectionException e) {
            if (!e.getMessage().contains("already exist"))
                throw new MessageQueueException("Failed to create column family for " + queueColumnFamily.getName(), e);
        }
        
        try {
            keyspace.createColumnFamily(this.keyIndexColumnFamily, ImmutableMap.<String, Object>builder()
                    .put("key_validation_class",     "UTF8Type")
                    .put("comparator_type",          "CompositeType(BytesType, UTF8Type)")
                    .put("read_repair_chance",       1.0)
                    .put("gc_grace_period",          0)     // TODO: Calculate gc_grace_period
                    .build());
        } catch (ConnectionException e) {
            if (!e.getMessage().contains("already exist"))
                throw new MessageQueueException("Failed to create column family for " + queueColumnFamily.getName(), e);
        }
    }
    
    @Override
    public void createQueue() throws MessageQueueException {
        try {
            // Convert the message object to JSON
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            mapper.writeValue(baos, settings);
            baos.flush();
            keyspace.prepareColumnMutation(queueColumnFamily, queueName, MessageQueueEntry.newMetadataEntry())
                    .putValue(baos.toByteArray(), null)
                    .execute();
        } catch (ConnectionException e) {
            throw new MessageQueueException("Failed to create column family for " + queueColumnFamily.getName(), e);
        } catch (Exception e) {
            throw new MessageQueueException("Error serializing queue settings " + queueColumnFamily.getName(), e);
        }
    }

    @Override
    public MessageConsumer createConsumer() {
        return new MessageConsumer() {
            private LinkedBlockingQueue<MessageQueueShard> workQueue    = Queues.newLinkedBlockingQueue();
            private LinkedBlockingQueue<MessageQueueShard> idleQueue    = Queues.newLinkedBlockingQueue();
            private long currentPartition = -1;
            
            {
                // Add all the shards to the idle queue
                List<MessageQueueShard> randomized = Lists.newArrayList(partitions);
                Collections.shuffle(randomized);
                idleQueue.addAll(randomized);
            }
            
            @Override
            public Collection<Message> readMessages(int itemsToPop) throws MessageQueueException, BusyLockException, InterruptedException {
                return readMessages(itemsToPop, 0, null);
            }         
            
            @Override
            public Collection<Message> readMessages(int itemsToPop, long timeout, TimeUnit units) throws MessageQueueException, BusyLockException, InterruptedException {
                long timeoutTime = (timeout == 0)
                                 ? 0 
                                 : System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(timeout,  units);
                
                // Whenever the partition changes we add all shards from this partition to the workQueue
                long partitionIndex = (TimeUnit.MICROSECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS)/settings.getPartitionDuration())%settings.getPartitionCount();
                if (partitionIndex != currentPartition) {
                    currentPartition = partitionIndex;
                    
                    List<MessageQueueShard> partitions = Lists.newArrayList();
                    idleQueue.drainTo(partitions);
                    for (MessageQueueShard partition : partitions) {
                        if (partition.getPartition() == currentPartition) {
                            workQueue.add(partition);
                        }
                        else {
                            idleQueue.add(partition);
                        }
                    }
                }
                
                // Loop while trying to get messages.
                // TODO: Make it possible to cancel this loop
                // TODO: Read full itemsToPop instead of just stopping when we get the first sucessful set
                Collection<Message> messages = null;
                while (true) {
                    // First, try an item from the work queue
                    if (!workQueue.isEmpty()) {
                        MessageQueueShard partition = workQueue.remove();
                        messages = readAndReturnShard(partition, itemsToPop);
                        if (messages != null && !messages.isEmpty()) 
                            return messages;
                        
                        // This is an old partition so we quickly skip to the next partition
                        if (partition.getPartition() != currentPartition) 
                            continue;
                    }
                    
                    // If we got here then the shard from the busy queue was empty
                    // so we try one of the idle shards, in case it has old, skipped data
                    if (!idleQueue.isEmpty()) {
                        messages = readAndReturnShard(idleQueue.remove(), itemsToPop);
                        if (messages != null && !messages.isEmpty()) 
                            return messages;
                    }

                    if (timeoutTime != 0 && System.currentTimeMillis() > timeoutTime) 
                        return Lists.newLinkedList();
                    
                    Thread.sleep(pollInterval);
                }
            }
            
            private Collection<Message> readAndReturnShard(MessageQueueShard partition, int itemsToPop) throws MessageQueueException, BusyLockException, InterruptedException {
                Collection<Message>   messages = null;
                if (partition != null) {
                    try {
                        if (partition.getLastCount() == 0) {
                            if (!this.hasMessages(partition.getName())) 
                                return null;
                        }
                        messages = internalReadMessages(partition.getName(), itemsToPop);
                        if (!messages.isEmpty()) {
                            return messages;
                        }
                        stats.incEmptyPartitionCount();
                    }
                    catch (BusyLockException e) {
//                        Thread.sleep(pollInterval);
                    }
                    finally {
                        if (partition.getPartition() == currentPartition ||
                            (messages != null && !messages.isEmpty())) {
                            workQueue.add(partition);
                        }
                        else {
                            idleQueue.add(partition);
                        }
                    }
                }
                return messages;
            }
            
            @Override
            public Collection<Message> peekMessages(int itemsToPop) throws MessageQueueException {
                long partitionIndex = (TimeUnit.MICROSECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS)/settings.getPartitionDuration())%settings.getPartitionCount();
                
                List<Message> messages = Lists.newArrayList();
                
                for (int i = 0; i < settings.getPartitionCount(); i++) {
                    for (int j = 0; j < settings.getShardCount(); j++) {
                        int index = (int) (((i + partitionIndex) % settings.getPartitionCount()) * settings.getShardCount() + j);
                        
                        messages.addAll(peekMessages(partitions.get(index).getName(), itemsToPop - messages.size()));
                        
                        if (messages.size() == itemsToPop)
                            return messages;
                    }
                }
                
                return messages;
            }
            
            private Collection<Message> peekMessages(String shardName, int itemsToPop) throws MessageQueueException {
                long curTimeMicros = TimeUnit.MICROSECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                
                try {
                    ColumnList<MessageQueueEntry> result = keyspace.prepareQuery(queueColumnFamily)
                            .setConsistencyLevel(consistencyLevel)
                            .getKey(shardName)
                            .withColumnRange(new RangeBuilder()
                                .setLimit(itemsToPop)
                                .setEnd(entrySerializer
                                        .makeEndpoint((byte)MessageQueueEntryType.Message.ordinal(), Equality.EQUAL)
                                        .append((byte)0, Equality.EQUAL)
                                        .append(TimeUUIDUtils.getMicrosTimeUUID(curTimeMicros), Equality.LESS_THAN_EQUALS).toBytes())
                                .build())
                        .execute()
                            .getResult();
                    
                    List<Message> messages = Lists.newArrayListWithCapacity(result.size());
                    for (Column<MessageQueueEntry> column : result) {
                        Message message = extractMessageFromColumn(column);
                        if (message != null)
                            messages.add(message);
                    }
                    return messages;
                } catch (ConnectionException e) {
                    throw new MessageQueueException("Error peeking for messages from shard " + shardName, e);
                }
            }
            
            private boolean hasMessages(String shardName) throws MessageQueueException {
                UUID currentTime = TimeUUIDUtils.getUniqueTimeUUIDinMicros();
                
                try {
                    ColumnList<MessageQueueEntry> result = keyspace.prepareQuery(queueColumnFamily)
                            .setConsistencyLevel(consistencyLevel)
                            .getKey(shardName)
                            .withColumnRange(new RangeBuilder()
                                .setLimit(1)   // Read extra messages because of the lock column
                                .setStart(entrySerializer
                                        .makeEndpoint((byte)MessageQueueEntryType.Message.ordinal(), Equality.EQUAL)
                                        .toBytes()
                                        )
                                .setEnd(entrySerializer
                                        .makeEndpoint((byte)MessageQueueEntryType.Message.ordinal(), Equality.EQUAL)
                                        .append((byte)0, Equality.EQUAL)
                                        .append(currentTime, Equality.LESS_THAN_EQUALS)
                                        .toBytes()
                                        )
                                .build())
                        .execute()
                            .getResult();
                    return !result.isEmpty();
                } catch (ConnectionException e) {
                    throw new MessageQueueException("Error checking shard for messages. " + shardName, e);
                }
            }
            
            private Collection<Message> internalReadMessages(String shardName, int itemsToPop) throws MessageQueueException, BusyLockException {
                List<Message>  entries       = Lists.newArrayList();
                MutationBatch m              = null;
                MessageQueueEntry lockColumn = null;
                ColumnListMutation<MessageQueueEntry> rowMutation = null;
                
                int lockColumnCount = 0;
                
                // Try locking first 
                try {
                    // 1. Write the lock column
                    lockColumn = MessageQueueEntry.newLockEntry(MessageQueueEntryState.None);
                    long curTimeMicros = TimeUUIDUtils.getTimeFromUUID(lockColumn.getTimestamp());
                    m = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                    m.withRow(queueColumnFamily, shardName)
                     .putColumn(lockColumn, curTimeMicros + lockTimeout, lockTtl.intValue());
                    m.execute();

                    // 2. Read back lock columns and entries
                    ColumnList<MessageQueueEntry> result = keyspace.prepareQuery(queueColumnFamily)
                            .setConsistencyLevel(consistencyLevel)
                            .getKey(shardName)
                            .withColumnRange(entrySerializer.buildRange()
                                    .greaterThanEquals((byte)MessageQueueEntryType.Lock.ordinal())
                                    .lessThanEquals((byte)MessageQueueEntryType.Lock.ordinal())
                                    .build())
                            .execute()
                            .getResult();
                    
                    m = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                    rowMutation = m.withRow(queueColumnFamily, shardName);
                    rowMutation.deleteColumn(lockColumn);
                    
                    int lockCount = 0;
                    boolean lockAcquired = false;
                    lockColumnCount = result.size();
                    for (Column<MessageQueueEntry> column : result) {
                        MessageQueueEntry lock = column.getName();
                        
                        if (lock.getType() == MessageQueueEntryType.Lock) {
                            lockColumnCount++;
                            // Stale lock so we can discard it
                            if (column.getLongValue() < curTimeMicros) {
                                stats.incExpiredLockCount();
                                rowMutation.deleteColumn(lock);
                            }
                            else if (lock.getState() == MessageQueueEntryState.Acquired) {
                                throw new BusyLockException("Not first lock");
                            }
                            // This is our lock
                            else {
                                lockCount++;
                                if (lockCount == 1 && lock.getTimestamp().equals(lockColumn.getTimestamp())) {
                                    lockAcquired = true;
                                }
                            }
                            
                            if (!lockAcquired) {
                                throw new BusyLockException("Not first lock");
                            }
                            
                            // Write the acquired lock column
                            lockColumn = MessageQueueEntry.newLockEntry(lockColumn.getTimestamp(), MessageQueueEntryState.Acquired);
                            rowMutation.putColumn(lockColumn, curTimeMicros + lockTimeout, lockTtl.intValue());
                        }
                    }
                }
                catch (BusyLockException e) {
                    stats.incLockContentionCount();
                    throw e;
                }
                catch (ConnectionException e) {
                    throw new MessageQueueException("Error", e);
                }
                finally {
                    try {
                        m.execute();
                    }
                    catch (Exception e) {
                        throw new MessageQueueException("Error committing lock", e);
                    }
                }
                    
                long curTimeMicros = TimeUUIDUtils.getMicrosTimeFromUUID(lockColumn.getTimestamp());
                
                m = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                
                // First, release the lock column
                rowMutation = m.withRow(queueColumnFamily, shardName);
                rowMutation.deleteColumn(lockColumn);
                
                // 2. Read back lock columns and entries
                try {
                    ColumnList<MessageQueueEntry> result = keyspace.prepareQuery(queueColumnFamily)
                            .setConsistencyLevel(consistencyLevel)
                            .getKey(shardName)
                            .withColumnRange(new RangeBuilder()
                                .setLimit(itemsToPop + lockColumnCount + 1)   // Read extra messages because of the lock column
                                .setEnd(entrySerializer
                                        .makeEndpoint((byte)MessageQueueEntryType.Message.ordinal(), Equality.EQUAL)
                                        .append((byte)0, Equality.EQUAL)
                                        .append(lockColumn.getTimestamp(), Equality.LESS_THAN_EQUALS).toBytes())
                                .build())
                        .execute()
                            .getResult();
                        
                    for (Column<MessageQueueEntry> column : result) {
                        if (itemsToPop == 0) {
                            break;
                        }
                        
                        MessageQueueEntry entry = column.getName();
                        switch (entry.getType()) {
                            case Lock: 
                                // TODO: Track number of locks read and make sure we don't exceed itemsToPop
                                // We have the lock
                                if (entry.getState() == MessageQueueEntryState.Acquired) {
                                    if (!entry.getTimestamp().equals(lockColumn.getTimestamp())) {
                                        throw new BusyLockException("Someone else snuck in");
                                    }
                                }
                                break;
                                
                            case Message: {
                                itemsToPop--; 
                                
                                // First, we always want to remove the old item
                                rowMutation.deleteColumn(entry);
                                
                                // Next, parse the message metadata and add a timeout entry
                                Message message = extractMessageFromColumn(column);
                                
                                // Update the message state
                                if (message != null) {
                                    if (message.hasKey()) {
                                        m.withRow(keyIndexColumnFamily, getCompositeKey(queueName, message.getKey()))
                                            .deleteColumn(MessageMetadataEntry.newMessageId(getCompositeKey(shardName, entry.getMessageId())));
                                    }
                                    
                                    entries.add(message);
                                    
                                    // Message has a timeout so we add a timeout event
                                    if (message.getTimeout() != 0) {
                                        MessageQueueEntry timeoutEntry = MessageQueueEntry.newMessageEntry(
                                                (byte)0,   // Timeout has to be of 0 priority otherwise it screws up the ordering of everything else
                                                TimeUUIDUtils.getMicrosTimeUUID(curTimeMicros + TimeUnit.MICROSECONDS.convert(message.getTimeout(), TimeUnit.SECONDS) + (counter.incrementAndGet() % 1000)), 
                                                MessageQueueEntryState.Busy);
                                        
                                        message.setToken(timeoutEntry.getTimestamp());
                                        
                                        m.withRow(queueColumnFamily, getShardKey(timeoutEntry))
                                         .putColumn(timeoutEntry, column.getStringValue(), settings.getRetentionTimeout());
                                        
                                        // Add the timeout column to the key
                                        if (message.hasKey()) {
                                            m.withRow(keyIndexColumnFamily, getCompositeKey(queueName, message.getKey()))
                                                .putEmptyColumn(
                                                        MessageMetadataEntry.newMessageId(getCompositeKey(getShardKey(timeoutEntry), timeoutEntry.getMessageId())),
                                                        settings.getRetentionTimeout());
                                        }
                                        
                                    }
                                    else {
                                        message.setToken(null);
                                    }

                                    if (message.hasTrigger()) {
                                        Trigger trigger = message.getTrigger().nextTrigger();
                                        if (trigger != null) {
                                            Message nextMessage = message.clone();
                                            nextMessage.setTrigger(trigger);
                                            fillMessageMutation(m, nextMessage);
                                        }
                                    }
                                    
                                    // Update some stats
                                    switch (entry.getState()) {
                                    case Waiting:
                                        stats.incProcessCount();
                                        break;
                                    case Busy:
                                        stats.incReprocessCount();
                                        break;
                                    default:
                                        // TODO:
                                        break;
                                    }
                                }
                                // The message metadata was invalid so we just get rid of it.
                                else {
                                    stats.incInvalidMessageCount();
                                }
                                break;
                            }
                            default: {
                                // TODO: Error: Unknown type
                                break;
                            }
                        }
                    }
                    
                    return entries;
                }
                catch (BusyLockException e) {
                    stats.incLockContentionCount();
                    throw e;
                }
                catch (Exception e) {
                    throw new MessageQueueException("Error processing queue shard : " + shardName, e);
                }
                // 3. Release lock and remove any acquired entries
                finally {
                    try {
                        m.execute();
                    }
                    catch (Exception e) {
                        throw new MessageQueueException("Error processing queue shard : " + shardName, e);
                    }
                }
            }
            
            @Override
            public void ackMessage(Message message) throws MessageQueueException {
                MutationBatch mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                fillAckMutation(message, mb);
                
                try {
                    mb.execute();
                } catch (ConnectionException e) {
                    throw new MessageQueueException("Failed to ack message", e);
                }
            }

            @Override
            public void ackMessages(Collection<Message> messages) throws MessageQueueException {
                MutationBatch mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                for (Message message : messages) {
                    fillAckMutation(message, mb);
                }
                
                try {
                    mb.execute();
                } catch (ConnectionException e) {
                    throw new MessageQueueException("Failed to ack messages", e);
                }
            }
            
            private void fillAckMutation(Message message, MutationBatch mb) {
                if (message.getToken() != null) {
                    MessageQueueEntry entry = getBusyEntry(message);
                    stats.incFinishMessageCount();
                    
                    // Remove timeout entry from the queue
                    mb.withRow(queueColumnFamily, getShardKey(entry))
                      .deleteColumn(entry);
                    
                    // Remove entry lookup from the key, if one exists
                    if (message.getKey() != null) {
                        mb.withRow(keyIndexColumnFamily, getCompositeKey(queueName, message.getKey()))
                            .deleteColumn(MessageMetadataEntry.newMessageId(entry.getMessageId()));
                    }
                    
                    // Run hooks to 
                    for (MessageQueueHooks hook : hooks) {
                        hook.beforeAckMessage(message, mb);
                    }
                }
            }

            private Message extractMessageFromColumn(Column<MessageQueueEntry> column) {
                // Next, parse the message metadata and add a timeout entry
                Message message = null;
                try {
                    ByteArrayInputStream bais = new ByteArrayInputStream(column.getByteArrayValue());
                    message = mapper.readValue(bais, Message.class);
                } catch (Exception e) {
                    // Error parsing the message so we pass it on to the invalid message handler.
                    try {
                        message = invalidMessageHandler.apply(column.getStringValue());
                    }
                    catch (Exception e2) {
                        // OK to ignore this
                    }
                } 
                return message;
            }

            @Override
            public void ackPoisonMessage(Message message) throws MessageQueueException {
                // TODO: Remove bad message and add to poison queue
                MutationBatch mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                fillAckMutation(message, mb);
                
                try {
                    mb.execute();
                } catch (ConnectionException e) {
                    throw new MessageQueueException("Failed to ack messages", e);
                }
            }

        };
    }

    @Override
    public MessageProducer createProducer() {
        return new MessageProducer() {
            @Override
            public String sendMessage(Message message) throws MessageQueueException {
                // TODO: Check for uniqueness
                MutationBatch mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                String messageId = fillMessageMutation(mb, message);

                try {
                    mb.execute();
                } catch (ConnectionException e) {
                    throw new MessageQueueException("Failed to insert message into queue", e);
                }
                
                return messageId;
            }
            
            @Override
            public Map<String, Message> sendMessages(Collection<Message> messages) throws MessageQueueException {
                // TODO: Check for uniqueness
                Map<String, Message> response = Maps.newLinkedHashMap();
                MutationBatch mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                for (Message message : messages) {
                    String messageId = fillMessageMutation(mb, message);
                    response.put(messageId, message);
                }
                
                try {
                    mb.execute();
                } catch (ConnectionException e) {
                    throw new MessageQueueException("Failed to insert messages into queue", e);
                }
                
                return response;
            }
        };
    }
    
    private String fillMessageMutation(MutationBatch mb, Message message) throws MessageQueueException {
        // Get the execution time from the message or set to current time so it runs immediately
        long curTimeMicros;
        if (!message.hasTrigger()) {
            curTimeMicros = TimeUnit.MICROSECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                          + (counter.incrementAndGet() % 1000);
        }
        else {
            curTimeMicros = TimeUnit.MICROSECONDS.convert(message.getTrigger().getTriggerTime(),  TimeUnit.MILLISECONDS)
                          + (counter.incrementAndGet() % 1000);
        }

        // Update the message for the new token
        message.setToken(TimeUUIDUtils.getMicrosTimeUUID(curTimeMicros));
        
        // Set up the queue entry
        MessageQueueEntry entry = MessageQueueEntry.newMessageEntry(
                message.getPriority(), 
                message.getToken(), 
                MessageQueueEntryState.Waiting);

        // Convert the message object to JSON
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            mapper.writeValue(baos, message);
            baos.flush();
        } catch (Exception e) {
            throw new MessageQueueException("Failed to serialize message data", e);
        }
        
        // Write the queue entry  
        String shardKey = getShardKey(entry);
        mb.withRow(queueColumnFamily, shardKey)
          .putColumn(entry, new String(baos.toByteArray()), (Integer)settings.getRetentionTimeout());
            
        // Write the lookup from queue key to queue entry
        if (message.hasKey()) {
            // TODO: Check for uniqueness
            mb.withRow(keyIndexColumnFamily, getCompositeKey(queueName, message.getKey()))
                .putEmptyColumn(MessageMetadataEntry.newMessageId(getCompositeKey(shardKey, entry.getMessageId())),
                                settings.getRetentionTimeout());
        }
        
        // Allow hook processing
        for (MessageQueueHooks hook : hooks) {
            hook.beforeSendMessage(message, mb);
        }
        
        // Update state and retun the token
        stats.incSendMessageCount();
        return getCompositeKey(shardKey, entry.getMessageId());
    }
}
