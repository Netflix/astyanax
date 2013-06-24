package com.netflix.astyanax.recipes.queue.shard.cassandra;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatchManager;
import com.netflix.astyanax.entitystore.CompositeEntityManager;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.queue.MessageQueueConstants;
import com.netflix.astyanax.recipes.queue.MessageQueueInfo;
import com.netflix.astyanax.recipes.queue.entity.MessageQueueEntry;
import com.netflix.astyanax.recipes.queue.exception.MessageQueueException;
import com.netflix.astyanax.recipes.queue.shard.ShardDirectoryDao;

/**
 * Store shard directory in Cassandra
 * 
 * RowKey:  QueueName
 * Column:  <ShardName><ShardStartTime>
 * Value:   null
 * 
 * @author elandau
 *
 */
public class CassandraShardDirectoryDao implements ShardDirectoryDao {
    private final static int SHARD_FINALIZED_TTL = (int)TimeUnit.SECONDS.convert(10, TimeUnit.DAYS);

    private final MessageQueueInfo                                     queueInfo;
    private final CompositeEntityManager<ShardDirectoryEntity, String> entityManager;
    private final CompositeEntityManager<MessageQueueEntry, String>    queueEntityManager;
    
    public CassandraShardDirectoryDao(
            Keyspace             keyspace, 
            MutationBatchManager batchManager,
            ConsistencyLevel     consistencyLevel, 
            MessageQueueInfo     queueInfo) {
        
        this.queueInfo    = queueInfo;
        
        entityManager = CompositeEntityManager.<ShardDirectoryEntity, String>builder()
                .withKeyspace(keyspace)
                .withColumnFamily(queueInfo.getColumnFamilyBase() + MessageQueueConstants.CF_DIRECTORY_SUFFIX)
                .withConsistency(consistencyLevel)
                .withMutationBatchManager(batchManager)
                .withEntityType(ShardDirectoryEntity.class)
                .build();
        
        queueEntityManager     = CompositeEntityManager.<MessageQueueEntry, String>builder()
                .withKeyspace(keyspace)
                .withColumnFamily(queueInfo.getColumnFamilyBase() + MessageQueueConstants.CF_QUEUE_SUFFIX)
                .withMutationBatchManager(batchManager)
                .withConsistency(consistencyLevel)
                .withEntityType(MessageQueueEntry.class)
                .build();

        
    }

    @Override
    public void createStorage() {
        entityManager.createStorage(null);
    }

    @Override
    public void registerShard(String shardId, long startTime) {
        this.entityManager.put(new ShardDirectoryEntity(this.queueInfo.getQueueName(), shardId, startTime));
    }

    @Override
    public void discardShard(String shardId) {
        this.entityManager.put(new ShardDirectoryEntity(this.queueInfo.getQueueName(), shardId, 0));
    }
    
    @Override
    public void finalizeShard(String shardName) {
        this.queueEntityManager.put(MessageQueueEntry.newFinalizedEntry(shardName, SHARD_FINALIZED_TTL));
    }

    @Override
    public Collection<String> listShards() throws MessageQueueException  {
        Collection<ShardDirectoryEntity> entries;
        try {
            entries = this.entityManager.createNativeQuery()
                    .whereId().equal(queueInfo.getQueueName())
                    .getResultSet();
        } catch (Exception e) {
            throw new MessageQueueException("Error listing shards for queue " + queueInfo.getQueueName(), e);
        }
        
        List<String> shards = Lists.newArrayList();
        for (ShardDirectoryEntity entry : entries) {
            shards.add(entry.getShardName());
        }
        return null;
    }
}
