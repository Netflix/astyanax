package com.netflix.astyanax.recipes.queue.persist;

import java.util.Collection;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatchManager;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.queue.MessageHistoryDao;
import com.netflix.astyanax.recipes.queue.MessageQueueInfo;
import com.netflix.astyanax.recipes.queue.entity.MessageHistoryEntry;

public class CassandraMessageHistoryDao implements MessageHistoryDao {

    public CassandraMessageHistoryDao(
            Keyspace             keyspace, 
            MutationBatchManager batchManager,
            ConsistencyLevel     consistencyLevel, 
            MessageQueueInfo     queueInfo) {
    }

    @Override
    public void deleteHistory(String key) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void createStorage() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void writeHistory(MessageHistoryEntry histroy) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public Collection<MessageHistoryEntry> readMessageHistory(String key,
            Long startTime, Long endTime, int count) {
        // TODO Auto-generated method stub
        return null;
    }

}
