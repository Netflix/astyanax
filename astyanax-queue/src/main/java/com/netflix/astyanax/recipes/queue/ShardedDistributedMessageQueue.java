package com.netflix.astyanax.recipes.queue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Equality;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.recipes.locks.BusyLockException;
import com.netflix.astyanax.recipes.queue.triggers.Trigger;
import com.netflix.astyanax.recipes.queue.shard.ModShardPolicy;
import com.netflix.astyanax.recipes.queue.shard.ShardReaderPolicy;
import com.netflix.astyanax.recipes.queue.shard.TimeModShardPolicy;
import com.netflix.astyanax.recipes.queue.shard.TimePartitionedShardReaderPolicy;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.retry.RunOnce;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.TimeUUIDSerializer;
import com.netflix.astyanax.util.RangeBuilder;
import com.netflix.astyanax.util.TimeUUIDUtils;

/**
 * ShardedDistributedMessageQueue is a Cassandra backed client driven message queue.  
 * 
 * Key features
 * 1.  Time partition circular row key set used to time bound how much a wide row can grow.  This,
 *      along with an aggressive gc_grace_seconds will give cassandra a chance to clear out the row
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
 * Column families:
 *  Queue
 *  KeyLookup
 *  History
 *  
 * @author elandau
 *
 */
public class ShardedDistributedMessageQueue implements MessageQueue {
    private static final Logger LOG = LoggerFactory.getLogger(ShardedDistributedMessageQueue.class);
    
    public static final char             COMPOSITE_ID_DELIMITER          = ':';
    public static final char             COMPOSITE_KEY_DELIMITER         = '$';
    public static final String           DEFAULT_COLUMN_FAMILY_NAME      = "Queues";
    public static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL       = ConsistencyLevel.CL_LOCAL_QUORUM;
    public static final RetryPolicy      DEFAULT_RETRY_POLICY            = RunOnce.get();
    public static final long             DEFAULT_LOCK_TIMEOUT            = TimeUnit.MICROSECONDS.convert(30,  TimeUnit.SECONDS);
    public static final Integer          DEFAULT_LOCK_TTL                = (int)TimeUnit.SECONDS.convert(2,   TimeUnit.MINUTES);
    public static final Boolean          DEFAULT_POISON_QUEUE_ENABLED    = false;
    public static final String           DEFAULT_QUEUE_SUFFIX            = "_queue";
    public static final String           DEFAULT_METADATA_SUFFIX         = "_metadata";
    public static final String           DEFAULT_HISTORY_SUFFIX          = "_history";
    public static final long             SCHEMA_CHANGE_DELAY             = 3000;
    public static final ImmutableMap<String, Object> DEFAULT_COLUMN_FAMILY_SETTINGS = ImmutableMap.<String, Object>builder()
            .put("read_repair_chance",       1.0)
            .put("gc_grace_seconds",         5)     // TODO: Calculate gc_grace_seconds
            .put("compaction_strategy",      "LeveledCompactionStrategy")
            .put("compaction_strategy_options", ImmutableMap.of("sstable_size_in_mb", "100"))
            .build();
    
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
        
        public Builder withTimeBuckets(int bucketCount, int bucketDuration, TimeUnit units) {
            queue.settings.setPartitionDuration(TimeUnit.MICROSECONDS.convert(bucketDuration,  units));
            queue.settings.setPartitionCount(bucketCount);
            return this;
        }
        
        /**
         * @deprecated Use withTimeBuckets instead
         */
        public Builder withBuckets(int bucketCount, int bucketDuration, TimeUnit units) {
            return withTimeBuckets(bucketCount, bucketDuration, units);
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
            queue.lockTtl = (int)TimeUnit.SECONDS.convert(ttl,  units);
            return this;
        }
        
        public Builder withPollInterval(Long internval, TimeUnit units) {
            queue.settings.setPollInterval(TimeUnit.MILLISECONDS.convert(internval,  units));
            return this;
        }
        
        public Builder withQueueName(String queueName) {
            queue.settings.setQueueName(queueName);
            return this;
        }
        
        public Builder withConsistencyLevel(ConsistencyLevel level) {
            queue.consistencyLevel = level;
            return this;
        }
        
        public Builder withColumnFamilySettings(Map<String, Object> settings) {
            queue.columnFamilySettings = settings;
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
        
        public Builder withModShardPolicy(ModShardPolicy policy) {
            queue.modShardPolicy = policy;
            return this;
        }
        
        public Builder withShardReaderPolicy(ShardReaderPolicy shardReaderPolicy) {
            queue.shardReaderPolicy = shardReaderPolicy;
            return this;
        }
        
        public ShardedDistributedMessageQueue build() throws MessageQueueException {
            queue.queueColumnFamily    = ColumnFamily.newColumnFamily(columnFamilyName + DEFAULT_QUEUE_SUFFIX,    StringSerializer.get(), entrySerializer); 
            queue.keyIndexColumnFamily = ColumnFamily.newColumnFamily(columnFamilyName + DEFAULT_METADATA_SUFFIX, StringSerializer.get(), metadataSerializer); 
            queue.historyColumnFamily  = ColumnFamily.newColumnFamily(columnFamilyName + DEFAULT_HISTORY_SUFFIX,  StringSerializer.get(), TimeUUIDSerializer.get()); 
            
            queue.initialize();
            return queue;
        }
    }

    // Immutable after configuration
    private ColumnFamily<String, MessageQueueEntry>     queueColumnFamily;
    private ColumnFamily<String, MessageMetadataEntry>  keyIndexColumnFamily;
    private ColumnFamily<String, UUID>                  historyColumnFamily;
    
    private Keyspace                        keyspace;
    private ConsistencyLevel                consistencyLevel    = DEFAULT_CONSISTENCY_LEVEL;
    private long                            lockTimeout         = DEFAULT_LOCK_TIMEOUT;
    private int                             lockTtl             = DEFAULT_LOCK_TTL;
    private MessageQueueStats               stats               = new CountingQueueStats();
    private AtomicLong                      counter             = new AtomicLong(new Random().nextInt(1000));
    private Collection<MessageQueueHooks>   hooks               = Lists.newArrayList();
    private MessageQueueSettings            settings            = new MessageQueueSettings();
    private Boolean                         bPoisonQueueEnabled = DEFAULT_POISON_QUEUE_ENABLED;
    private Map<String, Object>             columnFamilySettings = DEFAULT_COLUMN_FAMILY_SETTINGS;
    private ShardReaderPolicy               shardReaderPolicy;
    private ModShardPolicy                  modShardPolicy;
    private Function<String, Message>       invalidMessageHandler  = new Function<String, Message>() {
                                                                        @Override
                                                                        public Message apply(String input) {
                                                                            LOG.warn("Invalid message: " + input);
                                                                            return null;
                                                                        }
                                                                    };
    
    private void initialize() throws MessageQueueException {
        Preconditions.checkArgument(
                TimeUnit.SECONDS.convert(lockTimeout, TimeUnit.MICROSECONDS) < lockTtl, 
                "Timeout " + lockTtl + " seconds must be less than TTL " + TimeUnit.SECONDS.convert(lockTtl, TimeUnit.MICROSECONDS) + " seconds");
        Preconditions.checkNotNull(keyspace, "Must specify keyspace");
        
        try {
            Column<MessageQueueEntry> column = keyspace.prepareQuery(queueColumnFamily)
                    .setConsistencyLevel(consistencyLevel)
                    .getRow(settings.getQueueName())
                    .getColumn(MessageQueueEntry.newMetadataEntry())
                    .execute()
                    .getResult();
            
            ByteArrayInputStream bais = new ByteArrayInputStream(column.getByteArrayValue());
            settings = mapper.readValue(bais, MessageQueueSettings.class);
        } 
        catch (NotFoundException e) {
            LOG.info("Message queue metadata not found.  Queue does not exist in CF and will be created now.");
        }
        catch (BadRequestException e) { 
            if (e.isUnconfiguredColumnFamilyError()) {
                LOG.info("Column family does not exist.  Call createStorage() to create column family.");
            }
            else {
                throw new MessageQueueException("Error getting message queue metadata", e);
            }
        }
        catch (Exception e) {
            throw new MessageQueueException("Error getting message queue metadata", e);
        }
        
        if (shardReaderPolicy == null)
            shardReaderPolicy = new TimePartitionedShardReaderPolicy(settings);
        
        if (modShardPolicy == null)
            modShardPolicy = TimeModShardPolicy.getInstance();
    }

    /**
     * Return the shard for this message
     * @param message
     * @return
     */
    protected String getShardKey(Message message) {
        return getShardKey(message.getTokenTime(), this.modShardPolicy.getMessageShard(message, settings));
    }
    
    /**
     * Return the shard for this timestamp
     * @param message
     * @return
     */
    private String getShardKey(long messageTime, int modShard) {
        long timePartition;
        if (settings.getPartitionDuration() != null)
            timePartition = (messageTime / settings.getPartitionDuration()) % settings.getPartitionCount();
        else 
            timePartition = 0;
        return settings.getQueueName() + ":" + timePartition + ":" + modShard;
    }
    
    private String getCompositeKey(String name, String key) {
        return name + COMPOSITE_KEY_DELIMITER + key;
    }
    
    private static String[] splitCompositeKey(String key) throws MessageQueueException {
        String[] parts = StringUtils.split(key, COMPOSITE_KEY_DELIMITER);
        
        if (parts.length != 2) {
            throw new MessageQueueException("Invalid key '" + key + "'.  Expected format <queue|shard>$<name>. ");
        }

        return parts;
    }
    
    private <T> String serializeToString(T trigger) throws JsonGenerationException, JsonMappingException, IOException  {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        mapper.writeValue(baos, trigger);
        baos.flush();
        return baos.toString();
    }
    
    private <T> T deserializeString(String data, Class<T> clazz) throws JsonParseException, JsonMappingException, IOException {
        return (T) mapper.readValue(
                new ByteArrayInputStream(data.getBytes()), 
                clazz);
    }
    
    private <T> T deserializeString(String data, String className) throws JsonParseException, JsonMappingException, IOException, ClassNotFoundException {
        return (T) mapper.readValue(
                new ByteArrayInputStream(data.getBytes()), 
                Class.forName(className));
    }

    
    @Override
    public String getName() {
        return settings.getQueueName();
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
                    keys.add(settings.getQueueName() + ":" + i + ":" + j);
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

    @Override
    public void clearMessages() throws MessageQueueException {
        LOG.info("Clearing messages from '" + settings.getQueueName() + "'");
        MutationBatch mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
        
        for (MessageQueueShard partition : shardReaderPolicy.listShards()) {
            mb.withRow(queueColumnFamily, partition.getName()).delete();
        }
        
        try {
            mb.execute();
        } catch (ConnectionException e) {
            throw new MessageQueueException("Failed to clear messages from queue " + settings.getQueueName(), e);
        }
    }
    
    @Override
    public void deleteQueue() throws MessageQueueException {
        LOG.info("Deleting queue '" + settings.getQueueName() + "'");
        MutationBatch mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
        
        for (MessageQueueShard partition : shardReaderPolicy.listShards()) {
            mb.withRow(queueColumnFamily, partition.getName()).delete();
        }
        
        mb.withRow(queueColumnFamily, settings.getQueueName());
        
        try {
            mb.execute();
        } catch (ConnectionException e) {
            throw new MessageQueueException("Failed to clear messages from queue " + settings.getQueueName(), e);
        }
    }
    
    @Override
    public Message peekMessage(String messageId) throws MessageQueueException {
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
                LOG.warn("Error parsing message", e);
                // Error parsing the message so we pass it on to the invalid message handler.
                try {
                    return invalidMessageHandler.apply(column.getStringValue());
                }
                catch (Exception e2) {
                    LOG.warn("Error handling invalid message message", e2);
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
    public List<Message> peekMessagesByKey(String key) throws MessageQueueException {
        String groupRowKey = getCompositeKey(settings.getQueueName(), key);
        List<Message> messages = Lists.newArrayList();
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
                messages.add(peekMessage(entry.getName().getName()));
            }
        } catch (NotFoundException e) {
        } catch (ConnectionException e) {
            throw new MessageQueueException("Error fetching row " + groupRowKey, e);
        }    
        return messages;
    }
    
    @Override
    public Message peekMessageByKey(String key) throws MessageQueueException {
        String groupRowKey = getCompositeKey(settings.getQueueName(), key);
        try {
            ColumnList<MessageMetadataEntry> columns = keyspace.prepareQuery(keyIndexColumnFamily)
                .setConsistencyLevel(consistencyLevel)
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
                return peekMessage(entry.getName().getName());
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
        MutationBatch mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
        
        String groupRowKey = getCompositeKey(settings.getQueueName(), key);
        try {
            ColumnList<MessageMetadataEntry> columns = keyspace.prepareQuery(keyIndexColumnFamily)
                .setConsistencyLevel(consistencyLevel)
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
        MutationBatch mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
        
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

    private void changeSchema(Callable<Void> callable) throws MessageQueueException {
        for (int i = 0; i < 3; i++) {
            try {
                callable.call();
                try {
                    Thread.sleep(SCHEMA_CHANGE_DELAY);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new MessageQueueException("Interrupted while trying to create column family for queue " + this.settings.getQueueName(), ie);
                }
                return;
            } catch (SchemaDisagreementException e) {
                try {
                    Thread.sleep(SCHEMA_CHANGE_DELAY);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new MessageQueueException("Interrupted while trying to create column family for queue " + this.settings.getQueueName(), ie);
                }
            } catch (Exception e) {
                if (e.getMessage().contains("already exist"))
                    return;
                throw new MessageQueueException("Failed to create column family for " + queueColumnFamily.getName(), e);
            }
        }
    }
    
    @Override
    public void createStorage() throws MessageQueueException {
        changeSchema(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                keyspace.createColumnFamily(queueColumnFamily, ImmutableMap.<String, Object>builder()
                        .put("key_validation_class",     "UTF8Type")
                        .put("comparator_type",          "CompositeType(BytesType, BytesType(reversed=true), TimeUUIDType, TimeUUIDType, BytesType)")
                        .putAll(columnFamilySettings)
                        .build());
                return null;
            }            
        });
        
        changeSchema(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                keyspace.createColumnFamily(keyIndexColumnFamily, ImmutableMap.<String, Object>builder()
                        .put("key_validation_class",     "UTF8Type")
                        .put("comparator_type",          "CompositeType(BytesType, UTF8Type)")
                        .putAll(columnFamilySettings)
                        .build());
                return null;
            }
        });
        
        changeSchema(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                keyspace.createColumnFamily(historyColumnFamily, ImmutableMap.<String, Object>builder()
                        .put("default_validation_class", "UTF8Type")
                        .putAll(columnFamilySettings)
                        .build());
                return null;
            }
        });
    }
    
    @Override
    public void dropStorage() throws MessageQueueException {
        try {
            keyspace.dropColumnFamily(this.queueColumnFamily);
            try {
                Thread.sleep(SCHEMA_CHANGE_DELAY);
            } catch (InterruptedException e) {
            }
        } catch (ConnectionException e) {
            if (!e.getMessage().contains("already exist"))
                throw new MessageQueueException("Failed to create column family for " + queueColumnFamily.getName(), e);
        }
        
        try {
            keyspace.dropColumnFamily(this.keyIndexColumnFamily);
            try {
                Thread.sleep(SCHEMA_CHANGE_DELAY);
            } catch (InterruptedException e) {
            }
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
            keyspace.prepareColumnMutation(queueColumnFamily, settings.getQueueName(), MessageQueueEntry.newMetadataEntry())
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
            @Override
            public List<MessageContext> readMessages(int itemsToPop) throws MessageQueueException, BusyLockException, InterruptedException {
                return readMessages(itemsToPop, 0, null);
            }         
            
            @Override
            public List<MessageContext> readMessages(int itemsToPop, long timeout, TimeUnit units) throws MessageQueueException, BusyLockException, InterruptedException {
                long timeoutTime = (timeout == 0)
                                 ? 0 
                                 : System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(timeout,  units);
                
                // Loop while trying to get messages.
                // TODO: Make it possible to cancel this loop
                // TODO: Read full itemsToPop instead of just stopping when we get the first successful set
                List<MessageContext> messages = null;
                while (true) {
                    MessageQueueShard partition = shardReaderPolicy.nextShard();
                    if (partition != null) {
                        try {
                            messages = readAndReturnShard(partition, itemsToPop);
                            if (messages != null && !messages.isEmpty()) 
                                return messages;
                        }
                        finally {
                            shardReaderPolicy.releaseShard(partition, messages == null ? 0 : messages.size());
                        }
                    }                        
                    
                    if (timeoutTime != 0 && System.currentTimeMillis() > timeoutTime) 
                        return Lists.newLinkedList();
                }
            }
            
            @Override
            public List<Message> peekMessages(int itemsToPeek) throws MessageQueueException {
                return ShardedDistributedMessageQueue.this.peekMessages(itemsToPeek);
            }
            
            private List<MessageContext> readAndReturnShard(MessageQueueShard shard, int itemsToPop) throws MessageQueueException, BusyLockException, InterruptedException {
                List<MessageContext>   messages = null;
                try {
                    // Optimization to check without locking if the shard was previously empty
                    if (shard.getLastReadCount() == 0) {
                        if (!hasMessages(shard.getName())) {
                            return null;
                        }
                    }
                    
                    return readMessagesFromShard(shard.getName(), itemsToPop);
                }
                finally {
                    if (messages == null || messages.isEmpty()) 
                        stats.incEmptyPartitionCount();
                }
            }
            
            @Override
            public List<MessageContext> readMessagesFromShard(String shardName, int itemsToPop) throws MessageQueueException, BusyLockException {
                List<MessageContext> entries = Lists.newArrayList();
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
                     .putColumn(lockColumn, curTimeMicros + lockTimeout, lockTtl);
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
                            rowMutation.putColumn(lockColumn, curTimeMicros + lockTimeout, lockTtl);
                        }
                    }
                }
                catch (BusyLockException e) {
                    stats.incLockContentionCount();
                    throw e;
                }
                catch (ConnectionException e) {
                    LOG.error("Error reading shard " + shardName, e);
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
                                final Message message = extractMessageFromColumn(column);
                                
                                // Update the message state
                                if (message != null) {
                                    MessageContext context = new MessageContext();
                                    context.setMessage(message);
                                    
                                    if (message.hasKey()) {
                                        m.withRow(keyIndexColumnFamily, getCompositeKey(settings.getQueueName(), message.getKey()))
                                            .deleteColumn(MessageMetadataEntry.newMessageId(getCompositeKey(shardName, entry.getMessageId())));
                                        
                                        if (message.isKeepHistory()) {
                                            MessageHistory history = context.getHistory();
                                            history.setToken(entry.getTimestamp());
                                            history.setStartTime(curTimeMicros);
                                            history.setTriggerTime(message.getTrigger().getTriggerTime());
                                            history.setStatus(MessageStatus.RUNNING);
                                            try {
                                                m.withRow(historyColumnFamily, message.getKey())
                                                    .putColumn(entry.getTimestamp(), serializeToString(history), settings.getHistoryTtl());
                                            }
                                            catch (Exception e) {
                                                LOG.warn("Error serializing history for key '" + message.getKey() + "'", e);
                                            }
                                        }
                                    }
                                    
                                    // Message has a timeout so we add a timeout event.  
                                    if (message.getTimeout() != 0) {
                                        MessageQueueEntry timeoutEntry = MessageQueueEntry.newMessageEntry(
                                                (byte)0,   // Timeout has to be of 0 priority otherwise it screws up the ordering of everything else
                                                TimeUUIDUtils.getMicrosTimeUUID(curTimeMicros + TimeUnit.MICROSECONDS.convert(message.getTimeout(), TimeUnit.SECONDS) + (counter.incrementAndGet() % 1000)), 
                                                MessageQueueEntryState.Busy);
                                        
                                        message.setToken(timeoutEntry.getTimestamp());
                                        message.setRandom(timeoutEntry.getRandom());
                                        
                                        m.withRow(queueColumnFamily, getShardKey(message))
                                         .putColumn(timeoutEntry, column.getStringValue(), settings.getRetentionTimeout());
                                        
                                        MessageMetadataEntry messageIdEntry = MessageMetadataEntry.newMessageId(getCompositeKey(getShardKey(message), timeoutEntry.getMessageId()));
                                        
                                        // Add the timeout column to the key
                                        if (message.hasKey()) {
                                            m.withRow(keyIndexColumnFamily, getCompositeKey(settings.getQueueName(), message.getKey()))
                                                .putEmptyColumn(messageIdEntry, settings.getRetentionTimeout());
                                        }
                                        
                                        context.setAckMessageId(messageIdEntry.getName());
                                    }
                                    else {
                                        message.setToken(null);
                                    }
                                    
                                    if (message.hasTrigger()) {
                                        final Message nextMessage;
                                        Trigger trigger = message.getTrigger().nextTrigger();
                                        if (trigger != null) {
                                            nextMessage = message.clone();
                                            nextMessage.setTrigger(trigger);
                                            context.setNextMessage(nextMessage);
                                            
                                            if (message.isAutoCommitTrigger())
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
                                        LOG.warn("Unknown message state: " + entry.getState());
                                        // TODO:
                                        break;
                                    }
                                    
                                    entries.add(context);
                                }
                                // The message metadata was invalid so we just get rid of it.
                                else {
                                    stats.incInvalidMessageCount();
                                    // TODO: Add to poison queue
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
            public void ackMessage(MessageContext context) throws MessageQueueException {
                MutationBatch mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                fillAckMutation(context, mb);
                
                try {
                    mb.execute();
                } catch (ConnectionException e) {
                    throw new MessageQueueException("Failed to ack message", e);
                }
            }

            @Override
            public void ackMessages(Collection<MessageContext> messages) throws MessageQueueException {
                MutationBatch mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                for (MessageContext context : messages) {
                    fillAckMutation(context, mb);
                }
                
                try {
                    mb.execute();
                } catch (ConnectionException e) {
                    throw new MessageQueueException("Failed to ack messages", e);
                }
            }
            
            private void fillAckMutation(MessageContext context, MutationBatch mb) {
                stats.incAckMessageCount();
                
                Message message = context.getMessage();
                // Token refers to the timeout event.  If 0 (i.e. no) timeout was specified
                // then the token will not exist
                if (message.getToken() != null) {
                    MessageQueueEntry entry = MessageQueueEntry.newBusyEntry(message);
                    
                    // Remove timeout entry from the queue
                    mb.withRow(queueColumnFamily, getShardKey(message))
                      .deleteColumn(entry);
                    
                    // Remove entry lookup from the key, if one exists
                    if (message.hasKey()) {
                        mb.withRow(keyIndexColumnFamily, getCompositeKey(settings.getQueueName(), message.getKey()))
                            .deleteColumn(MessageMetadataEntry.newMessageId(entry.getMessageId()));
                        
                        if (message.isKeepHistory()) {
                            MessageHistory history = context.getHistory();
                            if (history.getStatus() == MessageStatus.RUNNING)
                                history.setStatus(MessageStatus.DONE);
                            history.setEndTime(TimeUnit.MICROSECONDS.convert(System.currentTimeMillis(),  TimeUnit.MILLISECONDS));
                            try {
                                mb.withRow(historyColumnFamily, message.getKey())
                                    .putColumn(history.getToken(),                      // Event time
                                               serializeToString(context.getHistory()), // History data
                                               settings.getHistoryTtl());               // TTL
                            } catch (Exception e) {
                                LOG.warn("Error serializing message history for " + message.getKey(), e);
                            }
                        }
                    }
                    
                    // Run hooks 
                    for (MessageQueueHooks hook : hooks) {
                        hook.beforeAckMessage(message, mb);
                    }
                }
                
                if (context.getNextMessage() != null) {
                    try {
                        fillMessageMutation(mb, context.getNextMessage());
                    } catch (MessageQueueException e) {
                        LOG.warn("Error filling nextMessage for " + message.getKey(), e);
                    }
                }
            }

            @Override
            public void ackPoisonMessage(MessageContext context) throws MessageQueueException {
                // TODO: Remove bad message and add to poison queue
                MutationBatch mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                fillAckMutation(context, mb);
                
                try {
                    mb.execute();
                } catch (ConnectionException e) {
                    stats.incPersistError();
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
                SendMessageResponse response = sendMessages(Lists.newArrayList(message));
                if (!response.getNotUnique().isEmpty()) 
                    throw new KeyExistsException("Key already exists ." + message.getKey());
                return Iterables.getFirst(response.getMessages().entrySet(), null).getKey();
            }
            
            @Override
            public SendMessageResponse sendMessages(Collection<Message> messages) throws MessageQueueException {
                Map<String, Message> uniqueKeys        = Maps.newHashMap();
                Set<String>          notUniqueKeys     = Sets.newHashSet();
                List<Message>        notUniqueMessages = Lists.newArrayList();

                MutationBatch mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                MessageMetadataEntry lockColumn = MessageMetadataEntry.newUnique();
                
                // Get list of keys that must be unique and prepare the mutation for phase 1
                for (Message message : messages) {
                    if (message.hasUniqueKey()) {
                        String groupKey = getCompositeKey(settings.getQueueName(), message.getKey());
                        uniqueKeys.put(groupKey, message);
                        mb.withRow(keyIndexColumnFamily, groupKey)
                            .putEmptyColumn(lockColumn, (Integer)lockTtl);
                    }
                }

                // We have some keys that need to be unique
                if (!uniqueKeys.isEmpty()) {
                    // Submit phase 1: Create a unique column for ALL of the unique keys
                    try {
                        mb.execute();
                    } catch (ConnectionException e) {
                        throw new MessageQueueException("Failed to check keys for uniqueness (1): " + uniqueKeys, e);
                    }
                    
                    // Phase 2: Read back ALL the lock columms
                    mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                    Rows<String, MessageMetadataEntry> result;
                    try {
                        result = keyspace.prepareQuery(keyIndexColumnFamily)
                            .setConsistencyLevel(consistencyLevel)
                            .getRowSlice(uniqueKeys.keySet())
                            .withColumnRange(metadataSerializer.buildRange()
                                    .greaterThanEquals((byte)MessageMetadataEntryType.Unique.ordinal())
                                    .lessThanEquals((byte)MessageMetadataEntryType.Unique.ordinal())
                                    .build())
                            .execute()
                            .getResult();
                    } catch (ConnectionException e) {
                        throw new MessageQueueException("Failed to check keys for uniqueness (2): " + uniqueKeys, e);
                    }
                    
                    for (Row<String, MessageMetadataEntry> row : result) {
                        // This key is already taken, roll back the check
                        if (row.getColumns().size() != 1) {
                            String messageKey = splitCompositeKey(row.getKey())[1];
                            
                            notUniqueKeys.add(messageKey);
                            notUniqueMessages.add(uniqueKeys.get(messageKey));
                            mb.withRow(keyIndexColumnFamily, row.getKey())
                                .deleteColumn(lockColumn);
                        }
                        // This key is now unique
                        else {
                            mb.withRow(keyIndexColumnFamily, row.getKey())
                                .putEmptyColumn(lockColumn);
                        }
                    }
                }
                
                // Commit the messages
                Map<String, Message> success = Maps.newLinkedHashMap();
                for (Message message : messages) {
                    if (message.hasKey() && notUniqueKeys.contains(message.getKey()))
                        continue;
                    
                    String messageId = fillMessageMutation(mb, message);
                    success.put(messageId, message);
                }
                
                try {
                    mb.execute();
                } catch (ConnectionException e) {
                    throw new MessageQueueException("Failed to insert messages into queue.", e);
                }
                
                return new SendMessageResponse(success, notUniqueMessages);
            }
        };
    }
    
    private String fillMessageMutation(MutationBatch mb, Message message) throws MessageQueueException {
        // Get the execution time from the message or set to current time so it runs immediately
        long curTimeMicros;
        if (!message.hasTrigger()) {
            curTimeMicros = TimeUnit.MICROSECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }
        else {
            curTimeMicros = TimeUnit.MICROSECONDS.convert(message.getTrigger().getTriggerTime(),  TimeUnit.MILLISECONDS);
        }
        curTimeMicros += (counter.incrementAndGet() % 1000);

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
            throw new MessageQueueException("Failed to serialize message data: " + message, e);
        }
        
        // Write the queue entry  
        String shardKey = getShardKey(message);
        mb.withRow(queueColumnFamily, shardKey)
          .putColumn(entry, new String(baos.toByteArray()), (Integer)settings.getRetentionTimeout());
            
        // Write the lookup from queue key to queue entry
        if (message.hasKey()) {
            mb.withRow(keyIndexColumnFamily, getCompositeKey(settings.getQueueName(), message.getKey()))
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

    /**
     * Return history for a single key for the specified time range
     * 
     * TODO:  honor the time range :)
     */
    @Override
    public List<MessageHistory> getKeyHistory(String key, Long startTime, Long endTime, int count) throws MessageQueueException {
        List<MessageHistory> list = Lists.newArrayList();
        ColumnList<UUID> columns;
        try {
            columns = keyspace.prepareQuery(historyColumnFamily)
                .setConsistencyLevel(consistencyLevel)
                .getRow(key)
                .execute()
                .getResult();
        } catch (ConnectionException e) {
            throw new MessageQueueException("Failed to load history for " + key, e);
        }
        
        for (Column<UUID> column : columns) {
            try {
                list.add(deserializeString(column.getStringValue(), MessageHistory.class));
            } catch (Exception e) {
                LOG.info("Error deserializing history entry", e);
            }
        }
        return list;
    }

    /**
     * Iterate through shards attempting to extract itemsToPeek items.  Will return 
     * once itemToPeek items have been read or all shards have been checked.
     * 
     * Note that this call does not take into account the message trigger time and
     * will likely return messages that aren't due to be executed yet.
     * @return List of items
     */
    @Override
    public List<Message> peekMessages(int itemsToPeek) throws MessageQueueException {
        List<Message> messages = Lists.newArrayList();
        
        for (MessageQueueShard shard : shardReaderPolicy.listShards()) {
            messages.addAll(peekMessages(shard.getName(), itemsToPeek - messages.size()));
            
            if (messages.size() == itemsToPeek)
                return messages;
        }
        
        return messages;
    }
    
    /**
     * Peek into messages contained in the shard.  This call does not take trigger time into account
     * and will return messages that are not yet due to be executed
     * @param shardName
     * @param itemsToPop
     * @return
     * @throws MessageQueueException
     */
    private Collection<Message> peekMessages(String shardName, int itemsToPeek) throws MessageQueueException {
        try {
            ColumnList<MessageQueueEntry> result = keyspace.prepareQuery(queueColumnFamily)
                    .setConsistencyLevel(consistencyLevel)
                    .getKey(shardName)
                    .withColumnRange(new RangeBuilder()
                        .setLimit(itemsToPeek)
                        .setStart(entrySerializer
                                .makeEndpoint((byte)MessageQueueEntryType.Message.ordinal(), Equality.GREATER_THAN_EQUALS)
                                .toBytes())
                        .setEnd(entrySerializer
                                .makeEndpoint((byte)MessageQueueEntryType.Message.ordinal(), Equality.LESS_THAN_EQUALS)
                                .toBytes())
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
    
    /**
     * Extract a message body from a column
     * @param column
     * @return
     */
    private Message extractMessageFromColumn(Column<MessageQueueEntry> column) {
        // Next, parse the message metadata and add a timeout entry
        Message message = null;
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(column.getByteArrayValue());
            message = mapper.readValue(bais, Message.class);
        } catch (Exception e) {
            LOG.warn("Error processing message ", e);
            try {
                message = invalidMessageHandler.apply(column.getStringValue());
            }
            catch (Exception e2) {
                LOG.warn("Error processing invalid message", e2);
            }
        } 
        return message;
    }

    /**
     * Fast check to see if a shard has messages to process
     * @param shardName
     * @throws MessageQueueException
     */
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

    @Override
    public Map<String, MessageQueueShardStats> getShardStats() {
        return shardReaderPolicy.getShardStats();
    }
}
