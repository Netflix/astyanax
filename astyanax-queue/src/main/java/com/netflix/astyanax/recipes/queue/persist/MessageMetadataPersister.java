package com.netflix.astyanax.recipes.queue.persist;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatchManager;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.entitystore.CompositeEntityManager;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.queue.KeyExistsException;
import com.netflix.astyanax.recipes.queue.Message;
import com.netflix.astyanax.recipes.queue.MessageContext;
import com.netflix.astyanax.recipes.queue.MessageQueueException;
import com.netflix.astyanax.recipes.queue.MessageQueueInfo;
import com.netflix.astyanax.recipes.queue.MessageQueueUtils;
import com.netflix.astyanax.recipes.queue.entity.MessageMetadataEntry;
import com.netflix.astyanax.recipes.queue.entity.MessageMetadataEntryType;

/**
 * 
 * @author elandau
 */
public class MessageMetadataPersister extends AbstractMessagePersister {
    private static final Logger LOG = LoggerFactory.getLogger(MessageMetadataPersister.class);
    
    private final static int        DEFAULT_TTL_TIMEOUT     = 120;
    private final static String     INFO_SUFFIX             = "_info";
    private final static String     BODY_FIELD              = "body";
            
    private final MessageQueueInfo               queueInfo;
    private final CompositeEntityManager<MessageMetadataEntry, String> entityManager;
    private final CompositeEntityManager<MessageMetadataEntry, String> sharedEntityManager;
    private final Keyspace keyspace;
    
    public MessageMetadataPersister(
            Keyspace              keyspace, 
            MutationBatchManager  batchManager, 
            ConsistencyLevel      consistencyLevel,
            MessageQueueInfo      queueInfo) {
        this.queueInfo    = queueInfo;
        this.keyspace     = keyspace;
        
        entityManager = CompositeEntityManager.<MessageMetadataEntry, String>builder()
                .withKeyspace(keyspace)
                .withColumnFamily(queueInfo.getColumnFamilyBase() + INFO_SUFFIX)
                .withConsistency(consistencyLevel)
                .withAutoCommit(false)
                .withEntityType(MessageMetadataEntry.class)
                .build();

        sharedEntityManager = CompositeEntityManager.<MessageMetadataEntry, String>builder()
                .withKeyspace(keyspace)
                .withColumnFamily(queueInfo.getColumnFamilyBase() + INFO_SUFFIX)
                .withConsistency(consistencyLevel)
                .withMutationBatchManager(batchManager)
                .withEntityType(MessageMetadataEntry.class)
                .build();

    }
    
    @Override
    public void readMessages(Collection<MessageContext> messages) {
        Map<String, MessageContext> messagesToRead = Maps.newHashMap();
        for (MessageContext message : messages) {
            if (message.getMessage().isCompact() && message.getMessage().hasKey()) {
                messagesToRead.put(message.getMessage().getKey(), message);
            }
        }
        
        if (!messagesToRead.isEmpty()) {
            // Read all messages in bulk
        }
    }

    @Override
    public void preSendMessages(Collection<MessageContext> messages) throws MessageQueueException {
        Map<String, MessageContext> uniqueKeys  = Maps.newHashMap();
        MessageMetadataEntry        lockColumn  = MessageMetadataEntry.newUnique(null, DEFAULT_TTL_TIMEOUT);
        
        // Get list of keys that must be unique and prepare the mutation for phase 1
        for (MessageContext context : messages) {
            if (!context.hasError()) {
                Message message = context.getMessage();
                if (message.hasKey()) {
                    String messageKey = queueInfo.getQueueName() + "$" + message.getKey();
                    if (message.hasUniqueKey()) {
                        uniqueKeys.put(messageKey, context);
                        entityManager.put(lockColumn.duplicateForKey(messageKey));
                    }
                    else {
                        try {
                            sharedEntityManager.put(lockColumn.duplicateForKey(messageKey));
                            sharedEntityManager.put(MessageMetadataEntry.newField(
                                    messageKey, 
                                    BODY_FIELD, 
                                    MessageQueueUtils.serializeToString(context.getMessage()), 
                                    queueInfo.getRetentionTimeout()));
                            sharedEntityManager.put(MessageMetadataEntry.newMessageId(
                                    messageKey, 
                                    context.getAckMessageId().getFullMessageId(), 
                                    queueInfo.getRetentionTimeout()));
                        } catch (Exception e) {
                            LOG.warn(e.getMessage(), e);
                            context.setException(new MessageQueueException(String.format("Failed to persist message metadta for '%s'", messageKey), e));
                        }
                    }
                }
            }
        }

        // Now set the TTL to 0 for the rows that we are going to commit.
        lockColumn.setTtl(queueInfo.getRetentionTimeout());
        
        // We have some keys that need to be unique
        if (!uniqueKeys.isEmpty()) {
            // Submit phase 1: Create a unique column for ALL of the unique keys
            try {
                entityManager.commit();
            } catch (Exception e) {
                throw new MessageQueueException("Failed to check keys for uniqueness (1): " + uniqueKeys, e);
            }
            
            // Phase 2: Read back ALL the lock columms
            Map<String, Collection<MessageMetadataEntry>> result;
            try {
                result = entityManager.createNativeQuery()
                        .whereId().in(uniqueKeys.keySet())
                        .whereColumn("type").equal((byte)MessageMetadataEntryType.Unique.ordinal())
                        .getResultSetById();
            } catch (Exception e) {
                try {
                    LOG.info("Error reading ids: " + keyspace.getKeyspaceProperties().toString());
                } catch (ConnectionException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                
                throw new MessageQueueException("Failed to check keys for uniqueness (2): " + uniqueKeys, e);
            }

            // Phase 3: Commit that these rows are unique.  A partial failure here
            // may end up resulting in a row being incorrectly marked as unique but not 
            // being fully added to system thereby making it's key unusable
            for (Entry<String, MessageContext> context : uniqueKeys.entrySet()) {
                Collection<MessageMetadataEntry> entries = result.get(context.getKey());
                if (entries == null) {
                    context.getValue().setException(new MessageQueueException(String.format("Failed to back unique column for '%s'", context.getKey())));
                }
                else {
                    MessageMetadataEntry localLockColumn = lockColumn.duplicateForKey(context.getKey());
                    // This key is already taken, roll back the check
                    if (entries.size() > 1) {
                        sharedEntityManager.remove(localLockColumn);
                        context.getValue().setException(new KeyExistsException(context.getKey()));
                        LOG.info(context.getValue().getError().toString());
                    }
                    // This key is now unique
                    else {
                        try {
                            localLockColumn.setTtl(queueInfo.getRetentionTimeout());
                            sharedEntityManager.put(localLockColumn);
                            sharedEntityManager.put(MessageMetadataEntry.newField(
                                    context.getKey(), 
                                    BODY_FIELD, 
                                    MessageQueueUtils.serializeToString(context.getValue().getMessage()), 
                                    queueInfo.getRetentionTimeout()));
                            sharedEntityManager.put(MessageMetadataEntry.newMessageId(
                                    context.getKey(), 
                                    context.getValue().getAckMessageId().getFullMessageId(), 
                                    queueInfo.getRetentionTimeout()));
    
                        } catch (Exception e) {
                            context.getValue().setException(new MessageQueueException(String.format("Failed to persist metadata for key '%s'", context.getKey()), e));
                            LOG.info(context.getValue().getError().toString());
                        }
                    }
                }
            }
        }
    }

    @Override
    public String getPersisterName() {
        return "uniqueness";
    }
}
