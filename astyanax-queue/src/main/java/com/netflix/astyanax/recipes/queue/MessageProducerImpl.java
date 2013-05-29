package com.netflix.astyanax.recipes.queue;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.MutationBatchManager;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.queue.entity.MessageQueueEntry;
import com.netflix.astyanax.recipes.queue.entity.MessageQueueEntryState;
import com.netflix.astyanax.recipes.queue.persist.MessageHistoryPersister;
import com.netflix.astyanax.recipes.queue.persist.MessageMetadataPersister;
import com.netflix.astyanax.recipes.queue.persist.MessagePersister;
import com.netflix.astyanax.recipes.queue.persist.MessageQueuePersister;
import com.netflix.astyanax.recipes.queue.shard.QueueShardPolicy;
import com.netflix.astyanax.recipes.queue.triggers.RunOnceTrigger;

public class MessageProducerImpl implements MessageProducer {
    private final List<MessagePersister>         persisters = Lists.newArrayList();
    private       QueueShardPolicy               shardPolicy;
    private final MutationBatchManager           batchManager;
    private final MessageQueueInfo               queueInfo;
    
    private static final AtomicLong counter = new AtomicLong();
    
    public MessageProducerImpl(
            QueueShardPolicy            shardPolicy, 
            Keyspace                    keyspace, 
            MutationBatchManager        batchManager, 
            ConsistencyLevel            consistencyLevel,
            MessageQueueInfo            queueInfo) {
        this.batchManager = batchManager;
        this.shardPolicy  = shardPolicy;
        this.queueInfo    = queueInfo;
        
        this.persisters.add(new MessageMetadataPersister(
                keyspace, 
                batchManager, 
                consistencyLevel, 
                queueInfo)); 
        this.persisters.add(new MessageQueuePersister(
                keyspace, 
                batchManager, 
                consistencyLevel, 
                queueInfo));
        this.persisters.add(new MessageHistoryPersister(
                keyspace, 
                batchManager, 
                consistencyLevel, 
                queueInfo)); 
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
                    TimeUnit.MICROSECONDS.convert(message.getTrigger().getTriggerTime(), TimeUnit.MILLISECONDS) + (counter.incrementAndGet() % 1000),
                    MessageQueueEntryState.Waiting, 
                    null);
            queueEntry.setTtl(queueInfo.getRetentionTimeout());
            contexts.add(new MessageContext(queueEntry, message));
        }
        
        try {
            for (MessagePersister persister : persisters) {
                persister.preSendMessages(contexts);
            }
        }
        finally {
            try {
                batchManager.commitSharedMutationBatch();
            } catch (ConnectionException e) {
                throw new MessageQueueException("Failed to commit batch for enqueue", e);
            }
        }
        
        return contexts;
    }
    
    String fillMessageMutation(MutationBatch mb, Message message) throws MessageQueueException {
//        // Get the execution time from the message or set to current time so it runs immediately
//        long curTimeMicros;
//        if (!message.hasTrigger()) {
//            curTimeMicros = TimeUnit.MICROSECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
//        }
//        else {
//            curTimeMicros = TimeUnit.MICROSECONDS.convert(message.getTrigger().getTriggerTime(),  TimeUnit.MILLISECONDS);
//        }
//        curTimeMicros += (queue.counter.incrementAndGet() % 1000);
//
//        // Update the message for the new token
//        message.setToken(TimeUUIDUtils.getMicrosTimeUUID(curTimeMicros));
//
//        // Set up the queue entry
//        MessageQueueEntry entry = MessageQueueEntry.newMessageEntry(
//                message.getPriority(),
//                message.getToken(),
//                MessageQueueEntryState.Waiting);
//
//        // Convert the message object to JSON
//        Map<String, Object> parameters = message.getParameters();
//        if (message.isCompact() && message.hasKey() && message.hasParameters()) {
//            message.setParameters(null);
//        }
//        
//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        try {
//            queue.mapper.writeValue(baos, message);
//            baos.flush();
//        } catch (Exception e) {
//            throw new MessageQueueException("Failed to serialize message data: " + message, e);
//        }
//        message.setParameters(parameters);
//
//        // Write the queue entry
//        String shardKey = queue.getShardKey(message);
//        mb.withRow(queue.queueColumnFamily, shardKey)
//          .putColumn(entry, new String(baos.toByteArray()), queue.queueInfo.getRetentionTimeout());
//
//        // Write the lookup from queue key to queue entry
//        if (message.hasKey()) {
//            ColumnListMutation<MessageMetadataEntry> metaRow = mb.withRow(queue.keyIndexColumnFamily, queue.getCompositeKey(queue.getName(), message.getKey()));
//            metaRow.putEmptyColumn(MessageMetadataEntry.newMessageId(queue.getCompositeKey(shardKey, entry.getMessageId())), queue.queueInfo.getRetentionTimeout());
//            
//            if (parameters != null) {
//                ByteArrayOutputStream params_baos = new ByteArrayOutputStream();
//                try {
//                    queue.mapper.writeValue(params_baos, message.getParameters());
//                    params_baos.flush();
//                } catch (Exception e) {
//                    throw new MessageQueueException("Failed to serialize message data: " + message, e);
//                }
//                metaRow.putCompressedColumn(
//                        MessageMetadataEntry.newField(queue.FIELD_PARAMETERS), 
//                        new String(params_baos.toByteArray()), 
//                        queue.queueInfo.getRetentionTimeout());
//            }
//        }
//
//        // Allow hook processing
//        for (MessageQueueHooks hook : queue.hooks) {
//            hook.beforeSendMessage(message, mb);
//        }
//
//        // Update state and retun the token
//        queue.stats.incSendMessageCount();
//        return queue.getCompositeKey(shardKey, entry.getMessageId());
        return null;
    }

}
