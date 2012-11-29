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

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;

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
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Equality;
import com.netflix.astyanax.recipes.locks.BusyLockException;
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
    public static final String           DEFAULT_COLUMN_FAMILY_NAME      = "Queues";
    public static final String           DEFAULT_QUEUE_NAME              = "Queue";
    public static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL       = ConsistencyLevel.CL_LOCAL_QUORUM;
    public static final RetryPolicy      DEFAULT_RETRY_POLICY            = RunOnce.get();
    public static final long             DEFAULT_LOCK_TIMEOUT            = TimeUnit.MICROSECONDS.convert(1,   TimeUnit.MINUTES);
    public static final long             DEFAULT_LOCK_TTL                = TimeUnit.MICROSECONDS.convert(10,  TimeUnit.MINUTES);
    public static final long             DEFAULT_POLL_WAIT               = TimeUnit.MILLISECONDS.convert(50,  TimeUnit.MILLISECONDS);
    public static final long             DEFAULT_VISIBILITY_TIMEOUT      = TimeUnit.SECONDS.convert(4,  TimeUnit.DAYS);
    public static final int              DEFAULT_SHARD_COUNT             = 1;
    public static final long             DEFAULT_BUCKET_DURATION         = TimeUnit.MICROSECONDS.convert(30,  TimeUnit.SECONDS);
    public static final int              DEFAULT_BUCKET_COUNT            = 1;
    
    private final ObjectMapper mapper;
    
    {
        mapper = new ObjectMapper();
        mapper.getSerializationConfig().setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
    }

    public static class Builder {
        private ShardedDistributedMessageQueue queue        = new ShardedDistributedMessageQueue();
        private String                      columnFamilyName = DEFAULT_COLUMN_FAMILY_NAME;

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
        
        public Builder withVisibilityTimeout(Long timeout, TimeUnit units) {
            queue.settings.setVisibilityTimeout(TimeUnit.SECONDS.convert(timeout,  units));
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
        
        public Builder withHooks(MessageQueueHooks hooks) {
            queue.hooks = hooks;
            return this;
        }
        
        public MessageQueue build() {
            queue.columnFamily = ColumnFamily.newColumnFamily(columnFamilyName, StringSerializer.get(), compositeSerializer); 
            queue.initialize();
            return queue;
        }
    }

    private final static AnnotatedCompositeSerializer<MessageQueueEntry> compositeSerializer = new AnnotatedCompositeSerializer<MessageQueueEntry>(MessageQueueEntry.class);
    
    // Immutable after configuration
    private ColumnFamily<String, MessageQueueEntry> columnFamily;
    private Keyspace                        keyspace;
    private ConsistencyLevel                consistencyLevel    = DEFAULT_CONSISTENCY_LEVEL;
    private long                            lockTimeout         = DEFAULT_LOCK_TIMEOUT;
    private Long                            lockTtl             = DEFAULT_LOCK_TTL;
    private long                            pollInterval        = DEFAULT_POLL_WAIT;
    private MessageQueueStats                      stats               = new CountingQueueStats();
    private String                          queueName           = DEFAULT_QUEUE_NAME;
    private AtomicLong                      counter             = new AtomicLong(new Random().nextInt(1000));
    private MessageQueueHooks                      hooks               = new BaseQueueHook();
    private Function<String, Message>       invalidMessageHandler  = new Function<String, Message>() {
                                                                        @Override
                                                                        public Message apply(@Nullable String input) {
                                                                            return null;
                                                                        }
                                                                    };
    private List<MessageQueueShard>        partitions;
    private MessageQueueSettings               settings            = new MessageQueueSettings();
    
    private void initialize() {
        Preconditions.checkArgument(
                lockTtl == null || lockTimeout < lockTtl, 
                "Timeout " + lockTtl + " must be less than TTL " + lockTtl);
        
        try {
            Column<MessageQueueEntry> column = keyspace.prepareQuery(columnFamily)
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
    protected String getQueueKey(MessageQueueEntry message) {
        return getQueueKey(TimeUUIDUtils.getMicrosTimeFromUUID(message.getTimestamp()));
    }
    
    /**
     * Return the shard for this timestamp
     * @param message
     * @return
     */
    private String getQueueKey(long messageTime) {
        long timePartition = (messageTime / settings.getPartitionDuration()) % settings.getPartitionCount();
        long shard         =  messageTime % settings.getShardCount();
        return queueName + ":" + timePartition + ":" + shard;
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
            result.putAll(keyspace.prepareQuery(columnFamily)
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
    public void createQueue() throws MessageQueueException {
        try {
            keyspace.createColumnFamily(this.columnFamily, ImmutableMap.<String, Object>builder()
                            .put("key_validation_class",     "UTF8Type")
                            .put("comparator_type",          "CompositeType(BytesType, BytesType, TimeUUIDType, BytesType)")
                            .put("read_repair_chance",       1.0)
                            .put("gc_grace_period",          0)     // TODO: Calculate gc_grace_period
                            .build());
        } catch (ConnectionException e) {
            throw new MessageQueueException("Failed to create column family for " + columnFamily.getName(), e);
        }
        
        try {
            // Convert the message object to JSON
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            mapper.writeValue(baos, settings);
            baos.flush();
            keyspace.prepareColumnMutation(columnFamily, queueName, MessageQueueEntry.newMetadataEntry())
                    .putValue(baos.toByteArray(), null)
                    .execute();
        } catch (ConnectionException e) {
            throw new MessageQueueException("Failed to create column family for " + columnFamily.getName(), e);
        } catch (Exception e) {
            throw new MessageQueueException("Error serializing queue settings " + columnFamily.getName(), e);
        }
    }

    @Override
    public MessageConsumer createConsumer() {
        return new MessageConsumer() {
            private LinkedBlockingQueue<MessageQueueShard> workQueue    = Queues.newLinkedBlockingQueue();
            private LinkedBlockingQueue<MessageQueueShard> idleQueue    = Queues.newLinkedBlockingQueue();
            private long                                currentPartition = -1;
            
            {
                // Add all the shards to the idle queue
                List<MessageQueueShard> randomized = Lists.newArrayList(partitions);
                Collections.shuffle(randomized);
                idleQueue.addAll(randomized);
            }
            
            @Override
            public Collection<Message> readMessages(int itemsToPop) throws MessageQueueException, BusyLockException, InterruptedException {
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
                while (true) {
                    Collection<Message>   messages = null;
                    MessageQueueShard partition = null;
                    
                    // First, try an item from the work queue
                    if (!workQueue.isEmpty()) {
                        partition = workQueue.remove();
                        if (partition != null) {
                            try {
                                messages = internalReadMessages(itemsToPop, partition.getName());
                                if (!messages.isEmpty()) {
                                    return messages;
                                }
                                stats.incEmptyPartitionCount();
                                
                                if (partition.getPartition() != currentPartition)
                                    continue;
                            }
                            catch (BusyLockException e) {
                                Thread.sleep(pollInterval);
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
                    }
                    
                    // If we got here then the shard from the busy queue was empty
                    // so we try one of the idle shards, in case it has old, skipped data
                    if (!idleQueue.isEmpty()) {
                        partition = idleQueue.remove();
                        if (partition != null) {
                            try {
                                messages = internalReadMessages(itemsToPop, partition.getName());
                                if (!messages.isEmpty()) {
                                    return messages;
                                }
                                stats.incEmptyPartitionCount();
                            }
                            catch (BusyLockException e) {
                                Thread.sleep(pollInterval);
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
                    }

                    Thread.sleep(pollInterval);
                }
            }
            
            private Collection<Message> internalReadMessages(int itemsToPop, String shardName) throws MessageQueueException, BusyLockException {
                List<Message>  entries       = Lists.newArrayList();
                MutationBatch m           = null;
                MessageQueueEntry lockColumn = null;
                ColumnListMutation<MessageQueueEntry> rowMutation = null;
                
                // Try locking first 
                try {
                    // 1. Write the lock column
                    lockColumn = MessageQueueEntry.newLockEntry(MessageQueueEntryState.None);
                    long curTimeMicros = TimeUUIDUtils.getTimeFromUUID(lockColumn.getTimestamp());
                    m = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                    m.withRow(columnFamily, shardName)
                     .putColumn(lockColumn, curTimeMicros + lockTimeout, lockTtl.intValue());
                    m.execute();

                    // 2. Read back lock columns and entries
                    ColumnList<MessageQueueEntry> result = keyspace.prepareQuery(columnFamily)
                            .setConsistencyLevel(consistencyLevel)
                            .getKey(shardName)
                            .withColumnRange(compositeSerializer.buildRange()
                                    .greaterThanEquals((short)MessageQueueEntryType.Lock.ordinal())
                                    .lessThanEquals((short)MessageQueueEntryType.Lock.ordinal())
                                    .build())
                            .execute()
                            .getResult();
                    
                    m = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                    rowMutation = m.withRow(columnFamily, shardName);
                    rowMutation.deleteColumn(lockColumn);
                    
                    int lockCount = 0;
                    boolean lockAcquired = false;
                    for (Column<MessageQueueEntry> column : result) {
                        MessageQueueEntry lock = column.getName();
                        
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
                rowMutation = m.withRow(columnFamily, shardName);
                rowMutation.deleteColumn(lockColumn);
                
                // 2. Read back lock columns and entries
                try {
                    ColumnList<MessageQueueEntry> result = keyspace.prepareQuery(columnFamily)
                            .setConsistencyLevel(consistencyLevel)
                            .getKey(shardName)
                            .withColumnRange(new RangeBuilder()
                                .setLimit(itemsToPop)
                                .setEnd(compositeSerializer
                                        .makeEndpoint((short)MessageQueueEntryType.Message.ordinal(), Equality.EQUAL)
                                        .append((short)0, Equality.EQUAL)
                                        .append(TimeUUIDUtils.getMicrosTimeUUID(curTimeMicros), Equality.LESS_THAN_EQUALS).toBytes())
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
                                
                                // Update the message state
                                if (message != null) {
                                    entries.add(message);
                                    
                                    if (message.getTimeout() != 0) {
                                        MessageQueueEntry timeoutEntry = MessageQueueEntry.newMessageEntry(
                                                entry.getPriority(),
                                                TimeUUIDUtils.getMicrosTimeUUID(curTimeMicros + TimeUnit.MICROSECONDS.convert(message.getTimeout(), TimeUnit.SECONDS)), 
                                                MessageQueueEntryState.Busy);
                                        
                                        message.setToken(timeoutEntry.getTimestamp());
                                        
                                        m.withRow(columnFamily, getQueueKey(timeoutEntry))
                                         .putColumn(timeoutEntry, column.getStringValue());
                                    }
                                    else {
                                        message.setToken(null);
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
                    
                    hooks.beforeAckMessages(entries, m);
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
                MessageQueueEntry entry = getBusyEntry(message);
                stats.incFinishMessageCount();
                
                MutationBatch mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                mb.withRow(columnFamily, getQueueKey(entry))
                    .deleteColumn(entry);
                
                hooks.beforeAckMessage(message, mb);
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
                    if (message.getToken() != null) {
                        MessageQueueEntry entry = getBusyEntry(message);
                        stats.incFinishMessageCount();
                        mb.withRow(columnFamily, getQueueKey(entry))
                          .deleteColumn(entry);
                    }
                }
                
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
            public UUID sendMessage(Message message) throws MessageQueueException {
                // Get the execution time from the message or set to current time so it runs immediately
                long curTimeMicros;
                if (message.getNextTriggerTime() == 0) {
                    curTimeMicros = TimeUnit.MICROSECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                                  + (counter.incrementAndGet() % 1000);
                }
                else {
                    curTimeMicros = TimeUnit.MICROSECONDS.convert(message.getNextTriggerTime(),  TimeUnit.SECONDS)
                                  + (counter.incrementAndGet() % 1000000);
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

                // Write the mutation 
                MutationBatch mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                mb.withRow(columnFamily, getQueueKey(entry))
                  .putColumn(entry, new String(baos.toByteArray()), (int)settings.getVisibilityTimeout());
                    
                hooks.beforeSendMessage(message, mb);
                try {
                    mb.execute();
                } catch (ConnectionException e) {
                    throw new MessageQueueException("Failed to insert message into queue", e);
                }
                
                // Update state and retun the token
                stats.incSendMessageCount();
                return message.getToken();
            }

            @Override
            public Map<Message, UUID> sendMessages(Collection<Message> messages) throws MessageQueueException {
                Map<Message, UUID> response = Maps.newLinkedHashMap();
                MutationBatch mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                for (Message message : messages) {
                    // Get the execution time from the message or set to current time so it runs immediately
                    long curTimeMicros;
                    if (message.getNextTriggerTime() == 0) {
                        curTimeMicros = TimeUnit.MICROSECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                                      + (counter.incrementAndGet() % 1000);
                    }
                    else {
                        curTimeMicros = TimeUnit.MICROSECONDS.convert(message.getNextTriggerTime(),  TimeUnit.SECONDS)
                                      + (counter.incrementAndGet() % 1000000);
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
    
                    // Write the mutation 
                    mb.withRow(columnFamily, getQueueKey(entry))
                      .putColumn(entry, new String(baos.toByteArray()), (int)settings.getVisibilityTimeout());
                        
                    hooks.beforeSendMessage(message, mb);
                    response.put(message,  message.getToken());
                    
                    // Update state and retun the token
                    stats.incSendMessageCount();
                }
                
                try {
                    mb.execute();
                } catch (ConnectionException e) {
                    throw new MessageQueueException("Failed to insert message into queue", e);
                }
                
                return response;
            }
        };
    }


    @Override
    public String getName() {
        return queueName;
    }

}
