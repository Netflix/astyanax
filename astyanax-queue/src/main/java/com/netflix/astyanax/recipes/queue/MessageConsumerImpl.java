package com.netflix.astyanax.recipes.queue;

import com.google.common.collect.Lists;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Equality;
import com.netflix.astyanax.model.RangeEndpoint;
import com.netflix.astyanax.recipes.locks.BusyLockException;
import com.netflix.astyanax.recipes.queue.triggers.Trigger;
import com.netflix.astyanax.util.RangeBuilder;
import com.netflix.astyanax.util.TimeUUIDUtils;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Message consumer implementation based on the sharded queue.
 *
 * @author pbhattacharyya
 */
class MessageConsumerImpl implements MessageConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(MessageConsumerImpl.class);

    private final ShardedDistributedMessageQueue queue;

    public MessageConsumerImpl(ShardedDistributedMessageQueue q) {
        this.queue = q;
    }

    @Override
    public List<MessageContext> readMessages(int itemsToPop) throws MessageQueueException, BusyLockException, InterruptedException {
        return readMessages(itemsToPop, 0, null);
    }

    @Override
    public List<MessageContext> readMessages(int itemsToPop, long timeout, TimeUnit units) throws MessageQueueException, BusyLockException, InterruptedException {
        long timeoutTime = (timeout == 0) ? 0 : System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(timeout, units);
        // Loop while trying to get messages.
        // TODO: Make it possible to cancel this loop
        // TODO: Read full itemsToPop instead of just stopping when we get the first successful set
        List<MessageContext> messages = null;
        while (true) {
            MessageQueueShard partition = queue.shardReaderPolicy.nextShard();
            if (partition != null) {
                try {
                    messages = readAndReturnShard(partition, itemsToPop);
                    if (messages != null && !messages.isEmpty()) {
                        return messages;
                    }
                } finally {
                    queue.shardReaderPolicy.releaseShard(partition, messages == null ? 0 : messages.size());
                }
            }
            if (timeoutTime != 0 && System.currentTimeMillis() > timeoutTime) {
                return Lists.newLinkedList();
            }
            Thread.sleep(queue.shardReaderPolicy.getPollInterval());
        }
    }

    @Override
    public List<Message> peekMessages(int itemsToPeek) throws MessageQueueException {
        return queue.peekMessages(itemsToPeek);
    }

    private List<MessageContext> readAndReturnShard(MessageQueueShard shard, int itemsToPop) throws MessageQueueException, BusyLockException, InterruptedException {
        List<MessageContext> messages = null;
        try {
            messages = readMessagesFromShard(shard.getName(), itemsToPop);
        } finally {
            if (messages == null || messages.isEmpty()) {
                queue.stats.incEmptyPartitionCount();
            }
        }
        return messages;
    }

    @Override
    public List<MessageContext> readMessagesFromShard(String shardName, int itemsToPop) throws MessageQueueException, BusyLockException {
        if(queue.lockManager != null) {
            return readMessagesFromShardUsingLockManager(shardName, itemsToPop);
        }
        return readMessagesFromShardUsingDefaultLock(shardName, itemsToPop);
    }

    List<MessageContext> readMessagesFromShardUsingLockManager(String shardName, int itemToPop) throws MessageQueueException, BusyLockException {
        ShardLock lock = null;
        try {
            lock = queue.lockManager.acquireLock(shardName);
            MutationBatch m = queue.keyspace.prepareMutationBatch().setConsistencyLevel(queue.consistencyLevel);
            ColumnListMutation<MessageQueueEntry> rowMutation = m.withRow(queue.queueColumnFamily, shardName);
            long curTimeMicros = TimeUUIDUtils.getMicrosTimeFromUUID(TimeUUIDUtils.getUniqueTimeUUIDinMicros());
            return readMessagesInternal(shardName, itemToPop, 0, null, rowMutation, m, curTimeMicros);
        } catch (BusyLockException e) {
            queue.stats.incLockContentionCount();
            throw e;
        } catch (Exception e) {
            LOG.error("Error reading shard " + shardName, e);
            throw new MessageQueueException("Error", e);
        } finally {
            queue.lockManager.releaseLock(lock);
        }
    }

    List<MessageContext> readMessagesFromShardUsingDefaultLock(String shardName, int itemsToPop) throws MessageQueueException, BusyLockException {
        MutationBatch m = null;
        MessageQueueEntry lockColumn = null;
        ColumnListMutation<MessageQueueEntry> rowMutation = null;
        int lockColumnCount = 0;
        // Try locking first
        try {
            // 1. Write the lock column
            lockColumn = MessageQueueEntry.newLockEntry(MessageQueueEntryState.None);
            long curTimeMicros = TimeUUIDUtils.getTimeFromUUID(lockColumn.getTimestamp());
            m = queue.keyspace.prepareMutationBatch().setConsistencyLevel(queue.consistencyLevel);
            m.withRow(queue.queueColumnFamily, shardName).putColumn(lockColumn, curTimeMicros + queue.lockTimeout, queue.lockTtl);
            m.execute();
            // 2. Read back lock columns and entries
            ColumnList<MessageQueueEntry> result = queue.keyspace.prepareQuery(queue.queueColumnFamily).setConsistencyLevel(queue.consistencyLevel).getKey(shardName)
                    .withColumnRange(ShardedDistributedMessageQueue.entrySerializer
                                                                                   .buildRange()
                                                                                   .greaterThanEquals((byte) MessageQueueEntryType.Lock.ordinal())
                                                                                   .lessThanEquals((byte) MessageQueueEntryType.Lock.ordinal())
                                                                                   .build()
                                                                   )
                    .execute()
                    .getResult();
            m = queue.keyspace.prepareMutationBatch().setConsistencyLevel(queue.consistencyLevel);
            rowMutation = m.withRow(queue.queueColumnFamily, shardName);
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
                        queue.stats.incExpiredLockCount();
                        rowMutation.deleteColumn(lock);
                    } else if (lock.getState() == MessageQueueEntryState.Acquired) {
                        throw new BusyLockException("Not first lock");
                    } else {
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
                    rowMutation.putColumn(lockColumn, curTimeMicros + queue.lockTimeout, queue.lockTtl);
                }
            }
        } catch (BusyLockException e) {
            queue.stats.incLockContentionCount();
            throw e;
        } catch (ConnectionException e) {
            LOG.error("Error reading shard " + shardName, e);
            throw new MessageQueueException("Error", e);
        } finally {
            try {
                m.execute();
            } catch (Exception e) {
                throw new MessageQueueException("Error committing lock", e);
            }
        }
        long curTimeMicros = TimeUUIDUtils.getMicrosTimeFromUUID(lockColumn.getTimestamp());
        m = queue.keyspace.prepareMutationBatch().setConsistencyLevel(queue.consistencyLevel);
        // First, release the lock column
        rowMutation = m.withRow(queue.queueColumnFamily, shardName);
        rowMutation.deleteColumn(lockColumn);
        return readMessagesInternal(shardName, itemsToPop, lockColumnCount, lockColumn, rowMutation, m, curTimeMicros);
    }

    @Override
    public void ackMessage(MessageContext context) throws MessageQueueException {
        MutationBatch mb = queue.keyspace.prepareMutationBatch().setConsistencyLevel(queue.consistencyLevel);
        fillAckMutation(context, mb);
        try {
            mb.execute();
        } catch (ConnectionException e) {
            throw new MessageQueueException("Failed to ack message", e);
        }
    }

    @Override
    public void ackMessages(Collection<MessageContext> messages) throws MessageQueueException {
        MutationBatch mb = queue.keyspace.prepareMutationBatch().setConsistencyLevel(queue.consistencyLevel);
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
        queue.stats.incAckMessageCount();
        Message message = context.getMessage();
        // Token refers to the timeout event.  If 0 (i.e. no) timeout was specified
        // then the token will not exist
        if (message.getToken() != null) {
            MessageQueueEntry entry = MessageQueueEntry.newBusyEntry(message);
            // Remove timeout entry from the queue
            mb.withRow(queue.queueColumnFamily, queue.getShardKey(message)).deleteColumn(entry);
            // Remove entry lookup from the key, if one exists
            if (message.hasKey()) {
                mb.withRow(queue.keyIndexColumnFamily, queue.getCompositeKey(queue.getName(), message.getKey()))
                        .putEmptyColumn(MessageMetadataEntry.newMessageId(queue.getCompositeKey(queue.getShardKey(message), entry.getMessageId())), queue.metadataDeleteTTL);
                if (message.isKeepHistory()) {
                    MessageHistory history = context.getHistory();
                    if (history.getStatus() == MessageStatus.RUNNING) {
                        history.setStatus(MessageStatus.DONE);
                    }
                    history.setEndTime(TimeUnit.MICROSECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS));
                    try {
                        mb.withRow(queue.historyColumnFamily, message.getKey())
                                .putColumn(history.getToken(), queue.serializeToString(context.getHistory()), queue.metadata.getHistoryTtl()); // TTL
                    } catch (Exception e) {
                        LOG.warn("Error serializing message history for " + message.getKey(), e);
                    }
                }
            }
            // Run hooks
            for (MessageQueueHooks hook : queue.hooks) {
                hook.beforeAckMessage(message, mb);
            }
        }
        if (context.getNextMessage() != null) {
            try {
                queue.fillMessageMutation(mb, context.getNextMessage());
            } catch (MessageQueueException e) {
                LOG.warn("Error filling nextMessage for " + message.getKey(), e);
            }
        }
    }

    @Override
    public void ackPoisonMessage(MessageContext context) throws MessageQueueException {
        // TODO: Remove bad message and add to poison queue
        MutationBatch mb = queue.keyspace.prepareMutationBatch().setConsistencyLevel(queue.consistencyLevel);
        fillAckMutation(context, mb);
        try {
            mb.execute();
        } catch (ConnectionException e) {
            queue.stats.incPersistError();
            throw new MessageQueueException("Failed to ack messages", e);
        }
    }

    private List<MessageContext> readMessagesInternal(String shardName,
                                                      int itemsToPop,
                                                      int lockColumnCount,
                                                      MessageQueueEntry lockColumn,
                                                      ColumnListMutation<MessageQueueEntry> rowMutation,
                                                      MutationBatch m,
                                                      long curTimeMicros) throws BusyLockException, MessageQueueException {

        try {
            List<MessageContext> entries = Lists.newArrayList();
            RangeEndpoint re = ShardedDistributedMessageQueue.entrySerializer
                                              .makeEndpoint((byte) MessageQueueEntryType.Message.ordinal(), Equality.EQUAL)
                                              .append((byte) 0, Equality.EQUAL);
            if(lockColumn!=null) {
                re.append(lockColumn.getTimestamp(), Equality.LESS_THAN_EQUALS);
            } else {
                re.append(TimeUUIDUtils.getMicrosTimeUUID(curTimeMicros), Equality.LESS_THAN_EQUALS);
            }

            ColumnList<MessageQueueEntry> result = queue.keyspace.prepareQuery(queue.queueColumnFamily)
                    .setConsistencyLevel(queue.consistencyLevel).getKey(shardName).
                    withColumnRange(new RangeBuilder()
                                        .setLimit(itemsToPop + (lockColumn == null? 0:(lockColumnCount + 1)))
                                        .setEnd(re.toBytes())
                                        .build()).execute().getResult();
            for (Column<MessageQueueEntry> column : result) {
                if (itemsToPop == 0) {
                    break;
                }
                MessageQueueEntry entry = column.getName();
                switch (entry.getType()) {
                    case Lock:
                        // TODO: Track number of locks read and make sure we don't exceed itemsToPop
                        // We have the lock
                        if (lockColumn != null && entry.getState() == MessageQueueEntryState.Acquired) {
                            if (!entry.getTimestamp().equals(lockColumn.getTimestamp())) {
                                throw new BusyLockException("Someone else snuck in");
                            }
                        }
                        break;
                    case Message:
                        {
                            try {
                                itemsToPop--;
                                // First, we always want to remove the old item
                                String messageId = queue.getCompositeKey(shardName, entry.getMessageId());
                                rowMutation.deleteColumn(entry);
                                // Next, parse the message metadata and add a timeout entry
                                final Message message = queue.extractMessageFromColumn(column);
                                // Update the message state
                                if (message != null) {
                                    MessageContext context = new MessageContext();
                                    context.setMessage(message);
                                    // Message has a trigger so we need to figure out if it is an
                                    // unfinished repeating trigger and re-add it.
                                    if (message.hasTrigger()) {
                                        // Read back all messageIds associated with this key and check to see if we have duplicates.
                                        String groupRowKey = queue.getCompositeKey(queue.getName(), message.getKey());
                                        try {
                                            // Use consistency level
                                            ColumnList<MessageMetadataEntry> columns = queue.keyspace.prepareQuery(queue.keyIndexColumnFamily).getRow(groupRowKey).withColumnRange(ShardedDistributedMessageQueue.metadataSerializer.buildRange().greaterThanEquals((byte) MessageMetadataEntryType.MessageId.ordinal()).lessThanEquals((byte) MessageMetadataEntryType.MessageId.ordinal()).build()).execute().getResult();
                                            MessageMetadataEntry mostRecentMessageMetadata = null;
                                            long mostRecentTriggerTime = 0;
                                            for (Column<MessageMetadataEntry> currMessageEntry : columns) {
                                                MessageQueueEntry pendingMessageEntry = MessageQueueEntry.fromMetadata(currMessageEntry.getName());
                                                if (currMessageEntry.getTtl() == 0) {
                                                    long currMessageTriggerTime = pendingMessageEntry.getTimestamp(TimeUnit.MICROSECONDS);
                                                    // First message we found, so treat as the most recent
                                                    if (mostRecentMessageMetadata == null) {
                                                        mostRecentMessageMetadata = currMessageEntry.getName();
                                                        mostRecentTriggerTime = currMessageTriggerTime;
                                                    } else {
                                                        // This message's trigger time is after what we thought was the most recent.
                                                        // Discard the previous 'most' recent and accept this one instead
                                                        if (currMessageTriggerTime > mostRecentTriggerTime) {
                                                            LOG.warn("Need to discard : " + entry.getMessageId() + " => " + mostRecentMessageMetadata.getName());
                                                            m.withRow(queue.keyIndexColumnFamily,
                                                                    queue.getCompositeKey(queue.getName(), message.getKey())).putEmptyColumn(mostRecentMessageMetadata, queue.metadataDeleteTTL);
                                                            mostRecentTriggerTime = currMessageTriggerTime;
                                                            mostRecentMessageMetadata = currMessageEntry.getName();
                                                        } else {
                                                            LOG.warn("Need to discard : " + entry.getMessageId() + " => " + currMessageEntry.getName());
                                                            m.withRow(queue.keyIndexColumnFamily,
                                                                    queue.getCompositeKey(queue.getName(), message.getKey())).putEmptyColumn(currMessageEntry.getName(), queue.metadataDeleteTTL);
                                                        }
                                                    }
                                                }
                                            }
                                            if (mostRecentMessageMetadata != null) {
                                                if (!mostRecentMessageMetadata.getName().endsWith(entry.getMessageId())) {
                                                    throw new DuplicateMessageException("Duplicate trigger for " + messageId);
                                                }
                                            }
                                        } catch (NotFoundException e) {
                                        } catch (ConnectionException e) {
                                            throw new MessageQueueException("Error fetching row " + groupRowKey, e);
                                        }
                                        // Update the trigger
                                        final Message nextMessage;
                                        Trigger trigger = message.getTrigger().nextTrigger();
                                        if (trigger != null) {
                                            nextMessage = message.clone();
                                            nextMessage.setTrigger(trigger);
                                            context.setNextMessage(nextMessage);
                                            if (message.isAutoCommitTrigger()) {
                                                queue.fillMessageMutation(m, nextMessage);
                                            }
                                        }
                                    }
                                    // Message has a key so we remove this item from the messages by key index.
                                    // A timeout item will be added later
                                    if (message.hasKey()) {
                                        m.withRow(queue.keyIndexColumnFamily,
                                                queue.getCompositeKey(queue.getName(), message.getKey()))
                                                .putEmptyColumn(MessageMetadataEntry.newMessageId(messageId), queue.metadataDeleteTTL);
                                        LOG.debug("Removing from key  :  " + queue.getCompositeKey(queue.getName(), message.getKey()) + " : " + messageId);
                                        if (message.isKeepHistory()) {
                                            MessageHistory history = context.getHistory();
                                            history.setToken(entry.getTimestamp());
                                            history.setStartTime(curTimeMicros);
                                            history.setTriggerTime(message.getTrigger().getTriggerTime());
                                            history.setStatus(MessageStatus.RUNNING);
                                            try {
                                                m.withRow(queue.historyColumnFamily, message.getKey()).putColumn(entry.getTimestamp(), queue.serializeToString(history)
                                                        , queue.metadata.getHistoryTtl());
                                            } catch (Exception e) {
                                                LOG.warn("Error serializing history for key '" + message.getKey() + "'", e);
                                            }
                                        }
                                    }
                                    // Message has a timeout so we add a timeout event.
                                    if (message.getTimeout() > 0) {
                                        MessageQueueEntry timeoutEntry = MessageQueueEntry.newMessageEntry((byte) 0,
                                                TimeUUIDUtils.getMicrosTimeUUID(curTimeMicros
                                                + TimeUnit.MICROSECONDS.convert(message.getTimeout(), TimeUnit.SECONDS)
                                                + (queue.counter.incrementAndGet() % 1000)), MessageQueueEntryState.Busy);
                                        message.setToken(timeoutEntry.getTimestamp());
                                        message.setRandom(timeoutEntry.getRandom());
                                        m.withRow(queue.queueColumnFamily, queue.getShardKey(message))
                                                .putColumn(timeoutEntry, column.getStringValue(), queue.metadata.getRetentionTimeout());
                                        MessageMetadataEntry messageIdEntry = MessageMetadataEntry.newMessageId(queue.getCompositeKey(queue.getShardKey(message), timeoutEntry.getMessageId()));
                                        // Add the timeout column to the key
                                        if (message.hasKey()) {
                                            m.withRow(queue.keyIndexColumnFamily, queue.getCompositeKey(queue.getName(), message.getKey()))
                                                    .putEmptyColumn(messageIdEntry, queue.metadata.getRetentionTimeout());
                                        }
                                        context.setAckMessageId(messageIdEntry.getName());
                                    } else {
                                        message.setToken(null);
                                    }
                                    // Update some stats
                                    switch (entry.getState()) {
                                        case Waiting:
                                            queue.stats.incProcessCount();
                                            break;
                                        case Busy:
                                            queue.stats.incReprocessCount();
                                            break;
                                        default:
                                            LOG.warn("Unknown message state: " + entry.getState());
                                            // TODO:
                                            break;
                                    }
                                    entries.add(context);
                                } else {
                                    queue.stats.incInvalidMessageCount();
                                    // TODO: Add to poison queue
                                }
                            } catch (DuplicateMessageException e) {
                                // OK to ignore this error.  All the proper columns will have been deleted in the batch.
                            }
                            break;
                        }
                    default:
                        {
                            // TODO: Error: Unknown type
                            break;
                        }
                }
            }
            return entries;
        } catch (BusyLockException e) {
            queue.stats.incLockContentionCount();
            throw e;
        } catch (Exception e) {
            throw new MessageQueueException("Error processing queue shard : " + shardName, e);
        } finally {
            try {
                m.execute();
            } catch (Exception e) {
                throw new MessageQueueException("Error processing queue shard : " + shardName, e);
            }
        }
    }

}
