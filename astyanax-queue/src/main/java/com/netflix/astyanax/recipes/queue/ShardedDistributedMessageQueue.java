package com.netflix.astyanax.recipes.queue;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatchManager;
import com.netflix.astyanax.SingleMutationBatchManager;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.locks.BusyLockException;
import com.netflix.astyanax.recipes.queue.entity.MessageHistoryEntry;
import com.netflix.astyanax.recipes.queue.entity.MessageMetadataEntry;
import com.netflix.astyanax.recipes.queue.entity.MessageQueueEntry;
import com.netflix.astyanax.recipes.queue.entity.MessageQueueEntryState;
import com.netflix.astyanax.recipes.queue.exception.DuplicateMessageException;
import com.netflix.astyanax.recipes.queue.exception.MessageQueueException;
import com.netflix.astyanax.recipes.queue.lock.CassandraShardLockManager;
import com.netflix.astyanax.recipes.queue.persist.CassandraMessageHistoryDao;
import com.netflix.astyanax.recipes.queue.persist.CassandraMessageMetadataDao;
import com.netflix.astyanax.recipes.queue.persist.CassandraMessageQueueDao;
import com.netflix.astyanax.recipes.queue.shard.ModShardPolicy;
import com.netflix.astyanax.recipes.queue.shard.QueueShardPolicy;
import com.netflix.astyanax.recipes.queue.shard.ShardReaderPolicy;
import com.netflix.astyanax.recipes.queue.shard.TimeModShardPolicy;
import com.netflix.astyanax.recipes.queue.shard.TimePartitionQueueShardPolicy;
import com.netflix.astyanax.recipes.queue.shard.TimePartitionedShardReaderPolicy;
import com.netflix.astyanax.recipes.queue.triggers.RunOnceTrigger;
import com.netflix.astyanax.recipes.queue.triggers.Trigger;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.retry.RunOnce;

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
    public static final long             SCHEMA_CHANGE_DELAY             = 3000;
    public static final String           FIELD_PARAMETERS                = "PARAMS";
    public static final ImmutableMap<String, Object> DEFAULT_COLUMN_FAMILY_SETTINGS = ImmutableMap.<String, Object>builder()
            .put("read_repair_chance",       1.0)
            .put("gc_grace_seconds",         5)     // TODO: Calculate gc_grace_seconds
            .put("compaction_strategy",      "SizeTieredCompactionStrategy")
            .put("sstable_compression",     "")
            .build();

    /**
     *
     * @author elandau
     */
    public static class Builder {
        private String                          columnFamilyName              = DEFAULT_COLUMN_FAMILY_NAME;
        private ShardLockManager                lockManager;

        private Keyspace                        keyspace;
        private ConsistencyLevel                consistencyLevel    = DEFAULT_CONSISTENCY_LEVEL;
        private long                            lockTimeout         = DEFAULT_LOCK_TIMEOUT;
        private int                             lockTtl             = DEFAULT_LOCK_TTL;
        private int                             metadataDeleteTTL   = DEFAULT_METADATA_DELETE_TTL;
        private Collection<MessageQueueHooks>   hooks               = Lists.newArrayList();
        private MessageQueueStats               stats              ;
        private Boolean                         bPoisonQueueEnabled = DEFAULT_POISON_QUEUE_ENABLED;
        private Map<String, Object>             columnFamilySettings = DEFAULT_COLUMN_FAMILY_SETTINGS;
        private ShardReaderPolicy.Factory       shardReaderPolicyFactory;
        private ModShardPolicy                  modShardPolicy;
        private MessageQueueInfo                queueInfo;
                                               
        public Builder() {
        }
        
        public Builder withQueue(MessageQueueInfo queueInfo) {
            this.queueInfo = queueInfo;
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

        public Builder withConsistencyLevel(ConsistencyLevel level) {
            this.consistencyLevel = level;
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
                public ShardReaderPolicy create(MessageQueueInfo metadata) {
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
            Preconditions.checkNotNull(queueInfo, "Must specify queue info.");
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
    
    public Builder builder() {
        return new Builder();
    }

    // Immutable after configuration
    final ShardLockManager                lockManager;
    final String                          columnFamilyName;
    final Keyspace                        keyspace;
    final ConsistencyLevel                consistencyLevel;
    final long                            lockTimeout;
    final int                             lockTtl;
    final int                             metadataDeleteTTL;
    final Collection<MessageQueueHooks>   hooks;
    final MessageQueueInfo                queueInfo;
    final Boolean                         bPoisonQueueEnabled;
    final Map<String, Object>             columnFamilySettings;
    final ShardReaderPolicy               shardReaderPolicy;
    final ModShardPolicy                  modShardPolicy;
    final Function<String, Message>       invalidMessageHandler  = new LoggingInvalidMessageHandler();
    final MessageQueueStats               stats;
    final QueueShardPolicy                shardPolicy;
    final MutationBatchManager            batchManager;
    final MessageQueueDao                 queueDao;
    final MessageMetadataDao              metadataDao;
    final MessageHistoryDao               historyDao;

    private ShardedDistributedMessageQueue(Builder builder) throws MessageQueueException {
        this.columnFamilyName     = builder.columnFamilyName;

        this.consistencyLevel     = builder.consistencyLevel;
        this.keyspace             = builder.keyspace;
        this.hooks                = builder.hooks;
        this.modShardPolicy       = builder.modShardPolicy;
        this.lockTimeout          = builder.lockTimeout;
        this.lockTtl              = builder.lockTtl;
        this.bPoisonQueueEnabled  = builder.bPoisonQueueEnabled;
        this.queueInfo            = builder.queueInfo;
        this.columnFamilySettings = builder.columnFamilySettings;
        this.metadataDeleteTTL    = builder.metadataDeleteTTL;
        this.stats                = builder.stats;
        this.shardReaderPolicy    = builder.shardReaderPolicyFactory.create(queueInfo);
        this.batchManager         = new SingleMutationBatchManager(keyspace, consistencyLevel);
        this.shardPolicy          = new TimePartitionQueueShardPolicy(modShardPolicy, queueInfo);
        
        if (builder.lockManager != null) 
            this.lockManager      = builder.lockManager;
        else 
            this.lockManager      = new CassandraShardLockManager(keyspace, batchManager, consistencyLevel);
        
        queueDao    = new CassandraMessageQueueDao(keyspace, batchManager, consistencyLevel, queueInfo, shardReaderPolicy);
        metadataDao = new CassandraMessageMetadataDao(keyspace, batchManager, consistencyLevel, queueInfo);
        historyDao  = new CassandraMessageHistoryDao(keyspace, batchManager, consistencyLevel, queueInfo);
    }

    @Override
    public String getName() {
        return queueInfo.getQueueName();
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
        return queueDao.getShardCounts();
    }

    @Override
    public void clearMessages() throws MessageQueueException {
        // TODO: Clear the 'key' metadata
        queueDao.clearMessages();
    }

    @Override
    public MessageContext peekMessage(String messageId) throws MessageQueueException {
        return queueDao.readMessage(messageId);
    }

    @Override
    public List<MessageContext> peekMessagesByKey(String key) throws MessageQueueException {
        List<MessageContext> messages = Lists.newArrayList();
//        String messageKey = getCompositeKey(getName(), key);
//        try {
//            Collection<MessageMetadataEntry> results = this.metadataDao.getMessageIdsForKey(messageKey);
//            
//            for (MessageMetadataEntry entry : results) {
//                MessageContext message = peekMessage(entry.getName());
//                if (message != null) {
//                    messages.add(message);
//                }
//                else {
//                    LOG.warn("No queue item for " + entry.getName());
//                }
//            }
//        } catch (Exception e) {
//            throw new MessageQueueException(String.format("Error fetching row '%s'", messageKey), e);
//        }
        
        return messages;
    }

    @Override
    public void deleteMessage(String messageId) throws MessageQueueException {
        deleteMessages(ImmutableList.of(messageId));
    }

    @Override
    public void deleteMessages(Collection<String> messageIds) throws MessageQueueException {
        for (String messageId : messageIds) {
            queueDao.deleteQueueEntry(new MessageQueueEntry(messageId));
        }
        
        try {
            batchManager.commitSharedMutationBatch();
        } catch (ConnectionException e) {
            throw new MessageQueueException(String.format("Error deleting message ids '%s'", messageIds), e);
        }

    }

    /**
     * Return history for a single key for the specified time range
     *
     * TODO:  honor the time range :)
     */
    @Override
    public Collection<MessageHistoryEntry> getKeyHistory(String key, Long startTime, Long endTime, int count) throws MessageQueueException {
        return historyDao.readMessageHistory(key, startTime, endTime, count);
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
    public List<MessageContext> peekMessages(int itemsToPeek) throws MessageQueueException {
        List<MessageContext> messages = Lists.newArrayList();

        for (MessageQueueShard shard : shardReaderPolicy.listShards()) {
            messages.addAll(queueDao.readMessages(shard.getName(), itemsToPeek - messages.size()));

            if (messages.size() >= itemsToPeek)
                return messages;
        }

        return messages;
    }

    @Override
    public Collection<MessageContext> readMessages(int itemsToPop) throws MessageQueueException, BusyLockException, InterruptedException {
        return readMessages(itemsToPop, 0, null);
    }

    @Override
    public Collection<MessageContext> readMessages(int itemsToPop, long timeout, TimeUnit units) throws MessageQueueException, BusyLockException, InterruptedException {
        long timeoutTime = (timeout == 0) ? 0 : System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(timeout, units);
        // Loop while trying to get messages.
        // TODO: Make it possible to cancel this loop
        // TODO: Read full itemsToPop instead of just stopping when we get the first successful set
        Collection<MessageContext> messages = null;
        while (true) {
            MessageQueueShard partition = shardReaderPolicy.nextShard();
            if (partition != null) {
                try {
                    messages = readAndReturnShard(partition, itemsToPop);
                    if (messages != null && !messages.isEmpty()) {
                        return messages;
                    }
                } finally {
                    shardReaderPolicy.releaseShard(partition, messages == null ? 0 : messages.size());
                }
            }
            if (timeoutTime != 0 && System.currentTimeMillis() > timeoutTime) {
                return Lists.newLinkedList();
            }
            Thread.sleep(shardReaderPolicy.getPollInterval());
        }
    }

    private Collection<MessageContext> readAndReturnShard(MessageQueueShard shard, int itemsToPop) throws MessageQueueException, BusyLockException, InterruptedException {
        Collection<MessageContext> messages = null;
        try {
            messages = readMessagesFromShard(shard.getName(), itemsToPop);
        } finally {
            if (messages == null || messages.isEmpty()) {
                stats.incEmptyPartitionCount();
            }
        }
        return messages;
    }

    /**
     * Read up to itemsToPop messages from the specified shard.  This operation
     * is done under a shard lock and will handle all message validation logic.
     * @param shardName
     * @param itemsToPop
     * @return
     * @throws MessageQueueException
     * @throws BusyLockException
     */
    private Collection<MessageContext> readMessagesFromShard(
            String shardName, 
            int itemsToPop) throws MessageQueueException, BusyLockException {
        
        ShardLock lock = null;
        try {
            lock = lockManager.acquireLock(shardName);
            return readMessagesInternal(shardName, itemsToPop, lock.getExtraMessagesToRead());
        } catch (BusyLockException e) {
            stats.incLockContentionCount();
            throw e;
        } catch (Exception e) {
            LOG.error("Error reading shard " + shardName, e);
            throw new MessageQueueException("Error", e);
        } finally {
            if (lock != null)
                lockManager.releaseLock(lock);
        }
    }

    /**
     * Reader messages under an assumed shard lock.
     * 
     * @param shardName
     * @param itemsToPop
     * @param lockColumnCount
     * @return
     * @throws BusyLockException
     * @throws MessageQueueException
     */
    private Collection<MessageContext> readMessagesInternal(
            String shardName,
            int itemsToPop,
            int lockColumnCount) throws BusyLockException, MessageQueueException {

        Collection<MessageContext> contexts = this.queueDao.readMessages(
                shardName, 
                System.currentTimeMillis(), TimeUnit.MILLISECONDS, 
                itemsToPop);
        
        // Iterate through all message and retrieve any additional information, pop them from the queue
        // and update state
        for (MessageContext context : contexts) {
            try {
                // First, 'pop' the event from the queue
                queueDao.deleteQueueEntry(context.getAckQueueEntry());
                
                Message           message = context.getMessage();
                MessageQueueEntry entry   = context.getAckQueueEntry();
                
                if (message.hasKey() && (message.isCompact() || message.hasTrigger())) {
                    // Read the message metadata
                    // TODO:  We may want to make this a lazy operation or at least a bulk option.
                    Collection<MessageMetadataEntry> metadata = metadataDao.getMetadataForKey(message.getKey());
                
                    MessageMetadataEntry mostRecentMessageId = null;
                    long mostRecentTriggerTime = 0;
                    
                    for (MessageMetadataEntry metadataEntry : metadata) {
                        switch (metadataEntry.getMetadataType()) {
                        case Lock:
                            break;
                        case Unique:
                            break;
                        case Field:
                            if ("body".equals(metadataEntry.getName())) {
                            // TODO:
                            }
                            break;
                        case MessageId:
                            if (message.hasTrigger()) {
                                MessageQueueEntry pendingMessageEntry = new MessageQueueEntry(metadataEntry.getName());
                                if (metadataEntry.getTtl() == 0) {
                                    long currMessageTriggerTime = pendingMessageEntry.getTimestamp(TimeUnit.MICROSECONDS);
                                    if (mostRecentMessageId == null) {
                                        // First message we found, so treat as the most recent
                                        mostRecentMessageId = metadataEntry;
                                        mostRecentTriggerTime = currMessageTriggerTime;
                                    } else {
                                        MessageMetadataEntry toDelete;
                                        // This message's trigger time is after what we thought was the most recent.
                                        // Discard the previous 'most' recent and accept this one instead
                                        if (currMessageTriggerTime > mostRecentTriggerTime) {
                                            LOG.warn("Need to discard : " + entry.getMessageId() + " => " + mostRecentMessageId);
                                            toDelete = mostRecentMessageId.duplicate();
                                            
                                            mostRecentTriggerTime = currMessageTriggerTime;
                                            mostRecentMessageId   = metadataEntry;
                                        } else {
                                            LOG.warn("Need to discard : " + entry.getMessageId() + " => " + metadataEntry);
                                            toDelete = metadataEntry;
                                        }
                                        
                                        toDelete.setTtl(metadataDeleteTTL);
                                        toDelete.setValue(null);
                                        metadataDao.deleteMetadata(toDelete);
                                    }
                                } 
                            }
                            break;
                        }
                    }
                
                    if (mostRecentMessageId != null) {
                        if (!mostRecentMessageId.getName().endsWith(entry.getMessageId())) {
                            throw new DuplicateMessageException(String.format("Duplicate trigger for '%s'", message.getKey()));
                        }
                    }
                
                    // Update the trigger and assign it to the 'next' message to be executed.
                    // The next message may be auto enqueued here or once the message is acked.
                    if (message.hasTrigger()) {
                        final Message nextMessage;
                        Trigger trigger = message.getTrigger().nextTrigger();
                        if (trigger != null) {
                            nextMessage = message.clone();
                            nextMessage.setTrigger(trigger);
                            context.setNextMessage(nextMessage);
                            if (message.isAutoCommitTrigger() && entry.getState() == MessageQueueEntryState.Waiting) {
                                MessageQueueEntry queueEntry = MessageQueueEntry.newMessageEntry(
                                        shardPolicy.getShardKey(nextMessage),
                                        nextMessage.getPriority(),
                                        TimeUnit.MICROSECONDS.convert(nextMessage.getTrigger().getTriggerTime(), TimeUnit.MILLISECONDS),
                                        MessageQueueEntryState.Waiting, 
                                        MessageQueueUtils.serializeToString(nextMessage),
                                        queueInfo.getRetentionTimeout());
                                
                                queueDao.writeMessage(new MessageContext(queueEntry, nextMessage));
                            }
                        }
                    }
                    
                    // Message has a key so we remove this item from the messages by key index.
                    // A timeout item will be added later
                    if (message.hasKey() && mostRecentMessageId != null) {
                        mostRecentMessageId.setTtl(metadataDeleteTTL);
                        mostRecentMessageId.setValue(null);
                        metadataDao.writeMetadata(mostRecentMessageId);
                        
                        LOG.debug(String.format("Removing from key '%s - %s'", mostRecentMessageId.getKey(), mostRecentMessageId.getName()));
                        
                        if (message.isKeepHistory()) {
                            // TODO: Enqueue history
                        }
                    }
                }
                
                // Message has a timeout so we add a timeout event.
                if (message.getTimeout() > 0) {
                    // TODO: Need to figure out the correct timeout id
                    MessageQueueEntry timeoutEntry = MessageQueueEntry.newBusyEntry(shardPolicy.getShardKey(message), message, entry, queueInfo.getRetentionTimeout());
                    
                    // Add the timeout column to the key
                    if (message.hasKey()) {
                        metadataDao.writeMetadata(MessageMetadataEntry.newMessageId(message.getKey(), entry.getFullMessageId(), queueInfo.getRetentionTimeout()));
                    }
                    context.setAckMessageId(timeoutEntry);
                } 
                else {
                    context.setAckMessageId(null);
                }
            }
            catch (MessageQueueException e) {
                LOG.error(String.format("Error processing message '%s'", context.getAckQueueEntry()), e);
                context.setException(e);
            }
            catch (Exception e) {
                LOG.error(String.format("Error processing message '%s'", context.getAckQueueEntry()), e);
                context.setException(new MessageQueueException("Unknown error popping message", e));
            }
            
            if (context.hasError()) {
                // TODO: Add to poison queue
            }
        }
        
        try {
            batchManager.commitSharedMutationBatch();
        } catch (Exception e) {
            throw new MessageQueueException("Error processing queue shard : " + shardName, e);
        }
        
        return contexts;
    }
    
    @Override
    public void ackMessage(MessageContext context) throws MessageQueueException {
        ackMessages(ImmutableList.of(context));
    }

    @Override
    public void ackMessages(Collection<MessageContext> contexts) throws MessageQueueException {
        try {
            for (MessageContext context : contexts) {
                if (context.getAckQueueEntry() != null) {
                    // Delete the timeout queue entry
                    queueDao.deleteQueueEntry(context.getAckQueueEntry());
                    
                    Message message = context.getMessage();
                    
                    if (context.getNextMessage() != null) {
//                        queueDao.writeMessage(context);
                    }
                    
                    if (message.hasKey()) {
                        // Delete the timeout queue entry from the metadata
                        metadataDao.deleteMetadata(MessageMetadataEntry.newMessageId(message.getKey(), context.getAckQueueEntry().getFullMessageId(), 0));
                        
                        if (message.isKeepHistory()) {
                            
                        }
                    }
                }
            }
        }
        finally {
            try {
                batchManager.commitSharedMutationBatch();
            } catch (ConnectionException e) {
                LOG.error("Failed to ack messages", e);
                throw new MessageQueueException("Failed to ack messages", e);
            }
        }
    }

    @Override
    public void ackPoisonMessage(MessageContext context) throws MessageQueueException {
        // TODO;
    }

    /**
     * Fast check to see if a shard has messages to process
     * @param shardName
     * @throws MessageQueueException
     */
    public boolean hasMessages(String shardName) throws MessageQueueException {
        return !queueDao.readMessages(shardName, System.currentTimeMillis(), TimeUnit.MILLISECONDS, 1).isEmpty();
    }

    @Override
    public MessageContext sendMessage(Message message) throws MessageQueueException {
        MessageContext context = Iterables.getFirst(sendMessages(Lists.newArrayList(message)), null);
        if (context.hasError())
            throw context.getError();
        return context;
    }

    @Override
    public Collection<MessageContext> sendMessages(Collection<Message> messages) throws MessageQueueException {
        // Generate Id's for the messages based on the sharding policy
        List<MessageContext> contexts = Lists.newArrayListWithCapacity(messages.size());
        for (Message message : messages) {
            // Get the execution time from the message or set to current time so it runs immediately
            if (!message.hasTrigger()) {
                message.setTrigger(RunOnceTrigger.builder().build());
            }
            
            MessageQueueEntry queueEntry = MessageQueueEntry.newMessageEntry(
                    shardPolicy.getShardKey(message),
                    message.getPriority(),
                    TimeUnit.MICROSECONDS.convert(message.getTrigger().getTriggerTime(), TimeUnit.MILLISECONDS),
                    MessageQueueEntryState.Waiting, 
                    null, 
                    queueInfo.getRetentionTimeout());
            contexts.add(new MessageContext(queueEntry, message));
        }
        
        // Persist the messages.  Order here is important since the metadataDao does the uniqueness constraint
        try {
            metadataDao.writeMessages(contexts);
            queueDao.writeMessages(contexts);
        }
        finally {
            try {
                batchManager.commitSharedMutationBatch();
            } catch (ConnectionException e) {
                throw new MessageQueueException("Failed to commit batch for enqueue", e);
            } finally {
                batchManager.discard();
            }
        }
        
        return contexts;
    }
    
    @Override
    public boolean deleteMessageByKey(String key) throws MessageQueueException {
        Collection<MessageMetadataEntry> ids = metadataDao.getMessageIdsForKey(key);
        if (ids != null) {
            for (MessageMetadataEntry entry : ids) {
                queueDao.deleteQueueEntry(new MessageQueueEntry(entry.getName()));
            }
            metadataDao.deleteMessage(key);
            historyDao.deleteHistory(key);
            
            try {
                batchManager.commitSharedMutationBatch();
            } catch (ConnectionException e) {
                LOG.error(String.format("Failed to delete message '%s'", key), e);
                throw new MessageQueueException(String.format("Failed to delete message '%s'", key), e);
            }
        }
        
        return true;
    }

    @Override
    public Map<String, MessageQueueShardStats> getShardStats() {
        return shardReaderPolicy.getShardStats();
    }
    
    public ShardReaderPolicy getShardReaderPolicy() {
        return shardReaderPolicy;
    }
}
