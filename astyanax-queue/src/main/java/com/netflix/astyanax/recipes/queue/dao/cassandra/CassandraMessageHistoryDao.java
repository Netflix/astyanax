package com.netflix.astyanax.recipes.queue.dao.cassandra;

import java.util.Collection;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatchManager;
import com.netflix.astyanax.entitystore.CompositeEntityManager;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.queue.MessageQueueConstants;
import com.netflix.astyanax.recipes.queue.MessageQueueInfo;
import com.netflix.astyanax.recipes.queue.dao.MessageHistoryDao;
import com.netflix.astyanax.recipes.queue.entity.MessageHistoryEntry;
import com.netflix.astyanax.recipes.queue.exception.MessageQueueException;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class CassandraMessageHistoryDao implements MessageHistoryDao {
    
    private final MessageQueueInfo queueInfo;
    private CompositeEntityManager<MessageHistoryEntry, String> entityManager;
    
    public CassandraMessageHistoryDao(
            Keyspace             keyspace, 
            MutationBatchManager batchManager,
            ConsistencyLevel     consistencyLevel, 
            MessageQueueInfo     queueInfo) {
        
        this.queueInfo    = queueInfo;
        
        entityManager = CompositeEntityManager.<MessageHistoryEntry, String>builder()
                .withKeyspace(keyspace)
                .withColumnFamily(queueInfo.getColumnFamilyBase() + MessageQueueConstants.CF_INFO_SUFFIX)
                .withConsistency(consistencyLevel)
                .withAutoCommit(false)
                .withEntityType(MessageHistoryEntry.class)
                .withKeyPrefix(queueInfo.getQueueName() + "$")
                .build();
    }

    @Override
    public void deleteHistory(String key) {
        this.entityManager.delete(key);
    }

    @Override
    public void createStorage() {
        entityManager.createStorage(null);
    }

    @Override
    public void writeHistory(MessageHistoryEntry history) {
        history.setTtl(queueInfo.getHistoryTtl());
        entityManager.put(history);
    }

    @Override
    public Collection<MessageHistoryEntry> readMessageHistory(String key, Long startTime, Long endTime, int count) throws MessageQueueException {
        try {
            return entityManager.createNativeQuery()
                    .whereId().equal(key)
                    .whereColumn("timestamp").greaterThanEqual(TimeUUIDUtils.getMicrosTimeUUID(startTime))
                    .whereColumn("timestamp").lessThan(TimeUUIDUtils.getMicrosTimeUUID(endTime))
                    .limit(count)
                    .getResultSet();
        } catch (Exception e) {
            throw new MessageQueueException("Failed to get history for '%s'", e);
        }
    }

}
