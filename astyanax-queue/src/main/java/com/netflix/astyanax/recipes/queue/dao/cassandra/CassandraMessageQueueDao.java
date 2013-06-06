package com.netflix.astyanax.recipes.queue.dao.cassandra;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatchManager;
import com.netflix.astyanax.entitystore.CompositeEntityManager;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.queue.Message;
import com.netflix.astyanax.recipes.queue.MessageContext;
import com.netflix.astyanax.recipes.queue.MessageQueueConstants;
import com.netflix.astyanax.recipes.queue.MessageQueueInfo;
import com.netflix.astyanax.recipes.queue.MessageQueueUtils;
import com.netflix.astyanax.recipes.queue.dao.MessageQueueDao;
import com.netflix.astyanax.recipes.queue.entity.MessageQueueEntry;
import com.netflix.astyanax.recipes.queue.entity.MessageQueueEntryType;
import com.netflix.astyanax.recipes.queue.exception.MessageQueueException;
import com.netflix.astyanax.recipes.queue.shard.ShardReaderPolicy;
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
public class CassandraMessageQueueDao implements MessageQueueDao {
    private final Logger LOG = LoggerFactory.getLogger(CassandraMessageQueueDao.class);
    
    private final CompositeEntityManager<MessageQueueEntry, String> entityManager;
    private final ShardReaderPolicy                                 shardReaderPolicy;
    
    public CassandraMessageQueueDao(
            Keyspace             keyspace, 
            MutationBatchManager batchManager,
            ConsistencyLevel     consistencyLevel,
            MessageQueueInfo     queueInfo,
            ShardReaderPolicy    shardReaderPolicy) {
        
        this.shardReaderPolicy = shardReaderPolicy;
        
        this.entityManager     = CompositeEntityManager.<MessageQueueEntry, String>builder()
            .withKeyspace(keyspace)
            .withColumnFamily(queueInfo.getColumnFamilyBase() + MessageQueueConstants.CF_QUEUE_SUFFIX)
            .withMutationBatchManager(batchManager)
            .withEntityType(MessageQueueEntry.class)
            .build();
    }
    
    @Override
    public void createStorage() {
        entityManager.createStorage(null);
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
    public Collection<MessageContext> readMessages(String shardName, int itemsToPeek, long upToThisTime, TimeUnit timeUnits) throws MessageQueueException {
        try {
            LOG.info(String.format("Reading messages up to '%d'", upToThisTime));
            Collection<MessageQueueEntry> entries = entityManager.createNativeQuery()
                .whereId()               .equal(shardName)
                .whereColumn("type")     .equal((byte)MessageQueueEntryType.Message.ordinal())
                .whereColumn("priority") .greaterThan((byte)0)
                .whereColumn("timestamp").lessThan(TimeUUIDUtils.getMicrosTimeUUID(TimeUnit.MICROSECONDS.convert(upToThisTime, timeUnits)))
                .limit(itemsToPeek)
                .getResultSet();
            
            return convertShardEntityToMessageList(entries);
        } catch (Exception e) {
            throw new MessageQueueException(String.format("Error reading shard '%s'", shardName), e);
        }
    }
    
    @Override
    public MessageContext readMessage(String messageId) throws MessageQueueException {
        MessageQueueEntry entry = new MessageQueueEntry(messageId);
        try {
            entry = entityManager.createNativeQuery()
                .whereId()               .equal(entry.getShardName())
                .whereColumn("type")     .equal(entry.getType())
                .whereColumn("priority") .equal(entry.getPriority())
                .whereColumn("timestamp").equal(entry.getTimestamp())
                .whereColumn("random")   .equal(entry.getRandom())
                .whereColumn("state")    .equal(entry.getState())
                .getSingleResult();
            return convertEntryToContext(entry);
        } catch (Exception e) {
            throw new MessageQueueException(String.format("Failed to laod message '%s'", messageId), e);
        }
    }
    
    @Override
    public void writeMessage(MessageContext context) throws MessageQueueException {
        Message message = context.getMessage();
        
        // TODO: Maybe move this out?
        if (context.hasError()) 
            return;
        
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
            
            context.getAckQueueEntry().setBodyFromString(msgBody);
            entityManager.put(context.getAckQueueEntry());
        } catch (IOException e) {
            context.setException(new MessageQueueException("Failed to serialize message data: " + message, e));
        }
    }
    
    @Override
    public void writeQueueEntry(MessageQueueEntry entry) {
        entityManager.put(entry);
    }

    @Override
    public void writeMessages(Collection<MessageContext> messages) throws MessageQueueException {
        for (MessageContext context : messages) {
            writeMessage(context);
        }
    }

    @Override
    public void deleteQueueEntry(MessageQueueEntry entry) {
        entityManager.remove(entry);
    }

    @Override
    public void ackMessagesById(Collection<String> messageIds) {
        for (String messageId : messageIds) {
            MessageQueueEntry entry = new MessageQueueEntry(messageId);
            entityManager.remove(entry);
        }
    }

    @Override
    public Map<String, Integer> getShardCounts() throws MessageQueueException {
        Collection<String> shardNames = shardReaderPolicy.listShardNames();
        
        try {
            return entityManager.createNativeQuery()
                    .whereId().in(shardNames)
                    .getResultSetCounts();
        } catch (Exception e) {
            throw new MessageQueueException("Failed to get counts", e);
        }
    }

    @Override
    public void clearMessages() throws MessageQueueException {
        Collection<String> shardNames = shardReaderPolicy.listShardNames();
        
        try {
            // TODO:  This will NOT delete the message metadata
            this.entityManager.delete(shardNames);
        } catch (Exception e) {
            throw new MessageQueueException("Failed to get counts", e);
        }
    }

    private Collection<MessageContext> convertShardEntityToMessageList(Collection<MessageQueueEntry> entries) {
        Collection<MessageContext> contexts = Lists.newArrayListWithCapacity(entries.size());
        for (MessageQueueEntry entry: entries) {
            contexts.add(convertEntryToContext(entry));
        }
        return contexts;
    }

    private MessageContext convertEntryToContext(MessageQueueEntry entry) {
        MessageContext context = new MessageContext(entry, null);
        try {
            context.setMessage(entry.getBodyAsMessage());
        } catch (Exception e) {
            context.setException(new MessageQueueException("Error parsing message", e));
            // TODO: Delete the message
            LOG.error(e.getMessage(), e);
        }
        return context;
    }
}
