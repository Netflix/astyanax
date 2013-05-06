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
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Equality;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
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
    public static final Integer          DEFAULT_METADATA_DELETE_TTL     = (int)TimeUnit.SECONDS.convert(2,  TimeUnit.SECONDS);
    public static final Boolean          DEFAULT_POISON_QUEUE_ENABLED    = false;
    public static final String           DEFAULT_QUEUE_SUFFIX            = "_queue";
    public static final String           DEFAULT_METADATA_SUFFIX         = "_metadata";
    public static final String           DEFAULT_HISTORY_SUFFIX          = "_history";
    public static final long             SCHEMA_CHANGE_DELAY             = 3000;
    public static final ImmutableMap<String, Object> DEFAULT_COLUMN_FAMILY_SETTINGS = ImmutableMap.<String, Object>builder()
            .put("read_repair_chance",       1.0)
            .put("gc_grace_seconds",         5)     // TODO: Calculate gc_grace_seconds
            .put("compaction_strategy",      "SizeTieredCompactionStrategy")
            .build();

    final static AnnotatedCompositeSerializer<MessageQueueEntry> entrySerializer
        = new AnnotatedCompositeSerializer<MessageQueueEntry>(MessageQueueEntry.class);
    final static AnnotatedCompositeSerializer<MessageMetadataEntry>   metadataSerializer
        = new AnnotatedCompositeSerializer<MessageMetadataEntry>(MessageMetadataEntry.class);

    static final ObjectMapper mapper = new ObjectMapper();

    {
        mapper.getSerializationConfig().setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
        mapper.enableDefaultTyping();
    }

    /**
     *
     * @author elandau
     */
    public static class Builder {
        private String          columnFamilyName              = DEFAULT_COLUMN_FAMILY_NAME;
        private ShardLockManager                            lockManager;

        private Keyspace                        keyspace;
        private ConsistencyLevel                consistencyLevel    = DEFAULT_CONSISTENCY_LEVEL;
        private long                            lockTimeout         = DEFAULT_LOCK_TIMEOUT;
        private int                             lockTtl             = DEFAULT_LOCK_TTL;
        private String                          queueName           = MessageQueueMetadata.DEFAULT_QUEUE_NAME;
        private int                             metadataDeleteTTL   = DEFAULT_METADATA_DELETE_TTL;
        private Collection<MessageQueueHooks>   hooks               = Lists.newArrayList();
        private MessageQueueMetadata            metadata            = new MessageQueueMetadata();
        private MessageQueueStats               stats              ;
        private Boolean                         bPoisonQueueEnabled = DEFAULT_POISON_QUEUE_ENABLED;
        private Map<String, Object>             columnFamilySettings = DEFAULT_COLUMN_FAMILY_SETTINGS;
        private ShardReaderPolicy.Factory       shardReaderPolicyFactory;
        private ModShardPolicy                  modShardPolicy;
                                               
        public Builder() {
            metadata.setQueueName(queueName);
        }
        
        public Builder withColumnFamily(String columnFamilyName) {
            this.columnFamilyName = columnFamilyName;
            return this;
        }

        public Builder withMetadata(MessageQueueMetadata metadata) {
            this.metadata = metadata;
            return this;
        }
        
        public Builder withShardCount(int count) {
            this.metadata.setShardCount(count);
            return this;
        }

        public Builder withTimeBuckets(int bucketCount, int bucketDuration, TimeUnit units) {
            this.metadata.setPartitionDuration(TimeUnit.MICROSECONDS.convert(bucketDuration,  units));
            this.metadata.setPartitionCount(bucketCount);
            return this;
        }

        /**
         * @deprecated Use withTimeBuckets instead
         */
        public Builder withBuckets(int bucketCount, int bucketDuration, TimeUnit units) {
            return withTimeBuckets(bucketCount, bucketDuration, units);
        }

        public Builder withRetentionTimeout(Long timeout, TimeUnit units) {
            this.metadata.setRetentionTimeout(timeout,  units);
            return this;
        }

        public Builder withLockTimeout(Long timeout, TimeUnit units) {
            this.lockTimeout = TimeUnit.MICROSECONDS.convert(timeout,  units);
            return this;
        }

        public Builder withLockTtl(Long ttl, TimeUnit units) {
            this.lockTtl = (int)TimeUnit.SECONDS.convert(ttl,  units);
            return this;
        }

        /**
         * Define this on the ShardReaderPolicy instead 
         * @param internval
         * @param units
         * @return
         */
        @Deprecated
        public Builder withPollInterval(Long internval, TimeUnit units) {
            this.metadata.setPollInterval(TimeUnit.MILLISECONDS.convert(internval,  units));
            return this;
        }

        public Builder withQueueName(String queueName) {
            this.metadata.setQueueName(queueName);
            return this;
        }

        public Builder withConsistencyLevel(ConsistencyLevel level) {
            this.consistencyLevel = level;
            return this;
        }

        public Builder withColumnFamilySettings(Map<String, Object> settings) {
            this.columnFamilySettings = settings;
            return this;
        }

        public Builder withKeyspace(Keyspace keyspace) {
            this.keyspace = keyspace;
            return this;
        }

        public Builder withStats(MessageQueueStats stats) {
            this.stats = stats;
            return this;
        }

        public Builder withHook(MessageQueueHooks hooks) {
            this.hooks.add(hooks);
            return this;
        }

        public Builder withHooks(Collection<MessageQueueHooks> hooks) {
            this.hooks.addAll(hooks);
            return this;
        }

        public Builder withPoisonQueue(Boolean enabled) {
            this.bPoisonQueueEnabled = enabled;
            return this;
        }

        public Builder withModShardPolicy(ModShardPolicy policy) {
            this.modShardPolicy = policy;
            return this;
        }

        public Builder withShardReaderPolicy(final ShardReaderPolicy shardReaderPolicy) {
            this.shardReaderPolicyFactory = new ShardReaderPolicy.Factory() {
                @Override
                public ShardReaderPolicy create(MessageQueueMetadata metadata) {
                    return shardReaderPolicy;
                }
            };
            return this;
        }
        
        public Builder withShardReaderPolicy(ShardReaderPolicy.Factory shardReaderPolicyFactory) {
            this.shardReaderPolicyFactory = shardReaderPolicyFactory;
            return this;
        }

        public Builder withShardLockManager(ShardLockManager mgr) {
            this.lockManager = mgr;
            return this;
        }

        public ShardedDistributedMessageQueue build() throws MessageQueueException {
            Preconditions.checkArgument(
                    TimeUnit.SECONDS.convert(lockTimeout, TimeUnit.MICROSECONDS) < lockTtl,
                    "Timeout " + lockTtl + " seconds must be less than TTL " + TimeUnit.SECONDS.convert(lockTtl, TimeUnit.MICROSECONDS) + " seconds");
            Preconditions.checkNotNull(keyspace, "Must specify keyspace");
            
            if (shardReaderPolicyFactory == null)
                shardReaderPolicyFactory = TimePartitionedShardReaderPolicy.Factory.builder().build();

            if (modShardPolicy == null)
                modShardPolicy = TimeModShardPolicy.getInstance();

            if (stats == null)
                stats = new CountingQueueStats();
            
            return new ShardedDistributedMessageQueue(this);
        }
    }

    // Immutable after configuration
    final ShardLockManager                            lockManager;
    final ColumnFamily<String, MessageQueueEntry>     queueColumnFamily;
    final ColumnFamily<String, MessageMetadataEntry>  keyIndexColumnFamily;
    final ColumnFamily<String, UUID>                  historyColumnFamily;

    final Keyspace                        keyspace;
    final ConsistencyLevel                consistencyLevel;
    final long                            lockTimeout;
    final int                             lockTtl;
    final int                             metadataDeleteTTL;
    final Collection<MessageQueueHooks>   hooks;
    final MessageQueueMetadata            metadata;
    final Boolean                         bPoisonQueueEnabled;
    final Map<String, Object>             columnFamilySettings;
    final ShardReaderPolicy               shardReaderPolicy;
    final ModShardPolicy                  modShardPolicy;
    final Function<String, Message>       invalidMessageHandler  = new Function<String, Message>() {
                                                                @Override
                                                                public Message apply(String input) {
                                                                    LOG.warn("Invalid message: " + input);
                                                                    return null;
                                                                }
                                                            };

    final MessageQueueStats               stats;
    final AtomicLong                      counter             = new AtomicLong(new Random().nextInt(1000));

    private ShardedDistributedMessageQueue(Builder builder) throws MessageQueueException {
        this.queueColumnFamily    = ColumnFamily.newColumnFamily(builder.columnFamilyName + DEFAULT_QUEUE_SUFFIX,    StringSerializer.get(), entrySerializer);
        this.keyIndexColumnFamily = ColumnFamily.newColumnFamily(builder.columnFamilyName + DEFAULT_METADATA_SUFFIX, StringSerializer.get(), metadataSerializer);
        this.historyColumnFamily  = ColumnFamily.newColumnFamily(builder.columnFamilyName + DEFAULT_HISTORY_SUFFIX,  StringSerializer.get(), TimeUUIDSerializer.get());

        this.consistencyLevel     = builder.consistencyLevel;
        this.keyspace             = builder.keyspace;
        this.hooks                = builder.hooks;
        this.modShardPolicy       = builder.modShardPolicy;
        this.lockManager          = builder.lockManager;
        this.lockTimeout          = builder.lockTimeout;
        this.lockTtl              = builder.lockTtl;
        this.bPoisonQueueEnabled  = builder.bPoisonQueueEnabled;
        this.metadata             = builder.metadata;
        this.columnFamilySettings = builder.columnFamilySettings;
        this.metadataDeleteTTL    = builder.metadataDeleteTTL;
        this.stats                = builder.stats;

        this.shardReaderPolicy    = builder.shardReaderPolicyFactory.create(metadata);

//        try {
//            Column<MessageQueueEntry> column = keyspace.prepareQuery(queueColumnFamily)
//                    .setConsistencyLevel(consistencyLevel)
//                    .getRow(getName())
//                    .getColumn(MessageQueueEntry.newMetadataEntry())
//                    .execute()
//                    .getResult();
//
//            ByteArrayInputStream bais = new ByteArrayInputStream(column.getByteArrayValue());
//            MessageQueueSettings existingSettings = mapper.readValue(bais, MessageQueueSettings.class);
//            
//            // TODO: Override some internal settings with those persisted in the queue metadata
//        }
//        catch (NotFoundException e) {
//            LOG.info("Message queue metadata not found.  Queue does not exist in CF and will be created now.");
//        }
//        catch (BadRequestException e) {
//            if (e.isUnconfiguredColumnFamilyError()) {
//                LOG.info("Column family does not exist.  Call createStorage() to create column family.");
//            }
//            else {
//                throw new MessageQueueException("Error getting message queue metadata", e);
//            }
//        }
//        catch (Exception e) {
//            throw new MessageQueueException("Error getting message queue metadata", e);
//        }
    }


    /**
     * Return the shard for this message
     * @param message
     * @return
     */
    String getShardKey(Message message) {
        return getShardKey(message.getTokenTime(), this.modShardPolicy.getMessageShard(message, metadata));
    }

    /**
     * Return the shard for this timestamp
     * @param message
     * @return
     */
    private String getShardKey(long messageTime, int modShard) {
        long timePartition;
        if (metadata.getPartitionDuration() != null)
            timePartition = (messageTime / metadata.getPartitionDuration()) % metadata.getPartitionCount();
        else
            timePartition = 0;
        return getName() + ":" + timePartition + ":" + modShard;
    }

    String getCompositeKey(String name, String key) {
        return name + COMPOSITE_KEY_DELIMITER + key;
    }

    private static String[] splitCompositeKey(String key) throws MessageQueueException {
        String[] parts = StringUtils.split(key, COMPOSITE_KEY_DELIMITER);

        if (parts.length != 2) {
            throw new MessageQueueException("Invalid key '" + key + "'.  Expected format <queue|shard>$<name>. ");
        }

        return parts;
    }

    <T> String serializeToString(T trigger) throws JsonGenerationException, JsonMappingException, IOException {
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

    @SuppressWarnings({ "unused", "unchecked" })
    private <T> T deserializeString(String data, String className) throws JsonParseException, JsonMappingException, IOException, ClassNotFoundException {
        return (T) mapper.readValue(
                new ByteArrayInputStream(data.getBytes()),
                Class.forName(className));
    }


    @Override
    public String getName() {
        return metadata.getQueueName();
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
            for (int i = 0; i < metadata.getPartitionCount(); i++) {
                for (int j = 0; j < metadata.getShardCount(); j++) {
                    keys.add(getName() + ":" + i + ":" + j);
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
        LOG.info("Clearing messages from '" + getName() + "'");
        MutationBatch mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);

        for (MessageQueueShard partition : shardReaderPolicy.listShards()) {
            mb.withRow(queueColumnFamily, partition.getName()).delete();
        }

        try {
            mb.execute();
        } catch (ConnectionException e) {
            throw new MessageQueueException("Failed to clear messages from queue " + getName(), e);
        }
    }

    @Override
    public void deleteQueue() throws MessageQueueException {
        LOG.info("Deleting queue '" + getName() + "'");
        MutationBatch mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);

        for (MessageQueueShard partition : shardReaderPolicy.listShards()) {
            mb.withRow(queueColumnFamily, partition.getName()).delete();
        }

        mb.withRow(queueColumnFamily, getName());

        try {
            mb.execute();
        } catch (ConnectionException e) {
            throw new MessageQueueException("Failed to clear messages from queue " + getName(), e);
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
        String groupRowKey = getCompositeKey(getName(), key);
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
                if (entry.getTtl() != 0)
                    continue;

                Message message = peekMessage(entry.getName().getName());
                if (message != null) {
                    messages.add(peekMessage(entry.getName().getName()));
                }
                else {
                    LOG.warn("No queue item for " + entry.getName());
                }
            }
        } catch (NotFoundException e) {
        } catch (ConnectionException e) {
            throw new MessageQueueException("Error fetching row " + groupRowKey, e);
        }
        return messages;
    }

    @Override
    public Message peekMessageByKey(String key) throws MessageQueueException {
        String groupRowKey = getCompositeKey(getName(), key);
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
                if (entry.getTtl() != 0)
                    continue;
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

        String groupRowKey = getCompositeKey(getName(), key);
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
                    throw new MessageQueueException("Interrupted while trying to create column family for queue " + getName(), ie);
                }
                return;
            } catch (SchemaDisagreementException e) {
                try {
                    Thread.sleep(SCHEMA_CHANGE_DELAY);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new MessageQueueException("Interrupted while trying to create column family for queue " + getName(), ie);
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
            mapper.writeValue(baos, metadata);
            baos.flush();
            keyspace.prepareColumnMutation(queueColumnFamily, getName(), MessageQueueEntry.newMetadataEntry())
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
        return new MessageConsumerImpl(this);
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
                        String groupKey = getCompositeKey(getName(), message.getKey());
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

    String fillMessageMutation(MutationBatch mb, Message message) throws MessageQueueException {
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
          .putColumn(entry, new String(baos.toByteArray()), metadata.getRetentionTimeout());

        // Write the lookup from queue key to queue entry
        if (message.hasKey()) {
            mb.withRow(keyIndexColumnFamily, getCompositeKey(getName(), message.getKey()))
                    .putEmptyColumn(MessageMetadataEntry.newMessageId(getCompositeKey(shardKey, entry.getMessageId())),
                    metadata.getRetentionTimeout());
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
    Message extractMessageFromColumn(Column<MessageQueueEntry> column) {
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
    public Map<String, MessageQueueShardStats>         getShardStats() {
        return shardReaderPolicy.getShardStats();
    }
    
    public ShardReaderPolicy getShardReaderPolicy() {
        return shardReaderPolicy;
    }

    public ColumnFamily<String, MessageQueueEntry>     getQueueColumnFamily() {
        return this.queueColumnFamily;
    }

    public ColumnFamily<String, MessageMetadataEntry>  getKeyIndexColumnFamily() {
        return this.keyIndexColumnFamily;
    }

    public ColumnFamily<String, UUID>                  getHistoryColumnFamily() {
        return this.historyColumnFamily;
    }

}
