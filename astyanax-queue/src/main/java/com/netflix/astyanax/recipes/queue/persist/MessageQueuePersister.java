package com.netflix.astyanax.recipes.queue.persist;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatchManager;
import com.netflix.astyanax.entitystore.CompositeEntityManager;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.queue.Message;
import com.netflix.astyanax.recipes.queue.MessageContext;
import com.netflix.astyanax.recipes.queue.MessageQueueException;
import com.netflix.astyanax.recipes.queue.MessageQueueInfo;
import com.netflix.astyanax.recipes.queue.MessageQueueShardReader;
import com.netflix.astyanax.recipes.queue.MessageQueueUtils;
import com.netflix.astyanax.recipes.queue.entity.MessageQueueEntry;
import com.netflix.astyanax.recipes.queue.entity.MessageQueueEntryType;
import com.netflix.astyanax.util.TimeUUIDUtils;

/**
 * Implementation for a cassandra based persistence layer both for writing a message and 
 * reading queue shards.  Note that this implementation provides read access to a single
 * shard and not the entire queue.  Prioritizing shards to read will be done at a higher
 * layer.
 * 
 * @author elandau
 *
 */
public class MessageQueuePersister extends AbstractMessagePersister implements MessageQueueShardReader {
    private final Logger LOG = LoggerFactory.getLogger(MessageQueuePersister.class);
    
    private final CompositeEntityManager<MessageQueueEntry, String> entityManager;
    
    public MessageQueuePersister(
            Keyspace             keyspace, 
            MutationBatchManager batchManager,
            ConsistencyLevel     consistencyLevel,
            MessageQueueInfo     queueInfo) {
        entityManager = CompositeEntityManager.<MessageQueueEntry, String>builder()
            .withKeyspace(keyspace)
            .withColumnFamily(queueInfo.getColumnFamilyBase() + "_queue")
            .withMutationBatchManager(batchManager)
            .withEntityType(MessageQueueEntry.class)
            .build();
    }
    
    @Override
    public void createStorage() {
        entityManager.createStorage(null);
    }

    @Override
    public void preSendMessages(Collection<MessageContext> messages) throws MessageQueueException {
        for (MessageContext context : messages) {
            Message message = context.getMessage();
            
            if (context.hasError()) 
                continue;
            
            // Set up the queue entry
            try {
                // Convert the message object to JSON.  
                String msgBody;
                Map<String, Object> parameters = message.getParameters();
                try {
                    // If isCompact then we don't want to serialize the parameters with the queue column.
                    // These will be written to the separate metadata key instead
                    if (message.isCompact() && message.hasKey() && message.hasParameters()) {
                        message.setParameters(null);
                    }
        
                    msgBody = MessageQueueUtils.serializeToString(message);
                }
                finally {
                    message.setParameters(parameters);
                }
                
                context.getAckMessageId().setBody(msgBody);
                entityManager.put(context.getAckMessageId());
            } catch (IOException e) {
                context.setException(new MessageQueueException("Failed to serialize message data: " + message, e));
            }
        }
    }

    @Override
    public void readMessages(Collection<MessageContext> messages) throws MessageQueueException {
        throw new IllegalStateException("It doesn't make sense to read message");
    }

    @Override
    public Collection<MessageContext> readMessages(String shardName, int itemsToPeek) throws MessageQueueException {
        try {
            Collection<MessageQueueEntry> entries = entityManager.createNativeQuery()
                .whereId().equal(shardName)
                .whereColumn("type").equal((byte)MessageQueueEntryType.Message.ordinal())
                .limit(itemsToPeek)
                .getResultSet();
            
            return convertShardEntityToMessageList(entries);
        } catch (Exception e) {
            throw new MessageQueueException(String.format("Error reading shard '%s'", shardName), e);
        }
    }

    @Override
    public void ackMessagesById(Collection<String> messageIds) {
        for (String messageId : messageIds) {
            MessageQueueEntry entry = new MessageQueueEntry(messageId);
            entityManager.remove(entry);
        }
    }

    @Override
    public Collection<MessageContext> readMessages(String shardName, long upToThisTime, int itemsToPeek) throws MessageQueueException {
        try {
            Collection<MessageQueueEntry> entries = entityManager.createNativeQuery()
                .whereId().equal(shardName)
                .whereColumn("type")     .equal((byte)MessageQueueEntryType.Message.ordinal())
                .whereColumn("priority") .greaterThan(0)
                .whereColumn("timestamp").lessThan(TimeUUIDUtils.getTimeUUID(upToThisTime))
                .limit(itemsToPeek)
                .getResultSet();
            
            return convertShardEntityToMessageList(entries);
        } catch (Exception e) {
            throw new MessageQueueException(String.format("Error reading shard '%s'", shardName), e);
        }
    }
    
    private Collection<MessageContext> convertShardEntityToMessageList(Collection<MessageQueueEntry> entries) {
        List<MessageContext> messages = Lists.newArrayListWithCapacity(entries.size());
        for (MessageQueueEntry entry : entries) {
            try {
                Message message = MessageQueueUtils.deserializeString(entry.getBody(), Message.class);
                messages.add(new MessageContext(entry, message));
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
        return messages;
    }

    @Override
    public String getPersisterName() {
        return "queue";
    }
}
