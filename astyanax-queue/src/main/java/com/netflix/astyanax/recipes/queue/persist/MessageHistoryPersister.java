package com.netflix.astyanax.recipes.queue.persist;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatchManager;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.queue.MessageQueueInfo;

public class MessageHistoryPersister extends AbstractMessagePersister {

    public MessageHistoryPersister(
            Keyspace             keyspace, 
            MutationBatchManager batchManager,
            ConsistencyLevel     consistencyLevel, 
            MessageQueueInfo     queueInfo) {
        
    }

    @Override
    public String getPersisterName() {
        return "history";
    }

}
