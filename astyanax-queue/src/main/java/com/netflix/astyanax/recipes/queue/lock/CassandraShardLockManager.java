package com.netflix.astyanax.recipes.queue.lock;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatchManager;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.entitystore.CompositeEntityManager;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.locks.BusyLockException;
import com.netflix.astyanax.recipes.queue.MessageQueueConstants;
import com.netflix.astyanax.recipes.queue.MessageQueueInfo;
import com.netflix.astyanax.recipes.queue.ShardLock;
import com.netflix.astyanax.recipes.queue.ShardLockManager;
import com.netflix.astyanax.recipes.queue.entity.MessageQueueEntry;
import com.netflix.astyanax.recipes.queue.entity.MessageQueueEntryState;
import com.netflix.astyanax.recipes.queue.entity.MessageQueueEntryType;
import com.netflix.astyanax.recipes.queue.exception.MessageQueueException;

public class CassandraShardLockManager implements ShardLockManager {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraShardLockManager.class);
    
    private MessageQueueInfo                                  queueInfo;
    private CompositeEntityManager<MessageQueueEntry, String> entityManager;

    private int  lockTimeout    = 60;
    private int  lockTtl        = 120;
    
    public static class CassandraShardLock implements ShardLock {
        final MessageQueueEntry entry;
        final int lockCount;
        
        public CassandraShardLock(MessageQueueEntry entry, int lockCount) {
            this.entry     = entry;
            this.lockCount = lockCount;
        }
        
        @Override
        public String getShardName() {
            return entry.getShardName();
        }

        @Override
        public int getExtraMessagesToRead() {
            return lockCount;
        }
    }
    
    public CassandraShardLockManager(
            Keyspace             keyspace, 
            MutationBatchManager batchManager,
            ConsistencyLevel     consistencyLevel) {
        this.entityManager     = CompositeEntityManager.<MessageQueueEntry, String>builder()
                .withKeyspace(keyspace)
                .withColumnFamily(queueInfo.getColumnFamilyBase() + MessageQueueConstants.CF_QUEUE_SUFFIX)
                .withMutationBatchManager(batchManager)
                .withEntityType(MessageQueueEntry.class)
                .build();

    }
    
    private Collection<MessageQueueEntry> getLockColumns(String shardName) throws Exception {
        // 2. Read back lock columns and entries
        return entityManager.createNativeQuery()
              .whereId().equal(shardName)
              .whereColumn("type").equal((byte) MessageQueueEntryType.Lock.ordinal())
              .getResultSet();
    }
    
    @Override
    public ShardLock acquireLock(String shardName) throws BusyLockException, MessageQueueException {
        MessageQueueEntry lockColumn = null;
        
        ShardLock shardLock = null;
        
        // Try locking first
        try {
            // 1. Write the lock column
            lockColumn = MessageQueueEntry.newLockEntry(shardName, MessageQueueEntryState.None, lockTimeout);
            long curTimeMicros = lockColumn.getTimestamp(TimeUnit.MICROSECONDS);
            entityManager.put(lockColumn);
            entityManager.commit();
            
            // 2. Read back lock columns and entries
            Collection<MessageQueueEntry> result = getLockColumns(shardName);
            
            entityManager.remove(lockColumn);
            
            int lockCount = 0;
            boolean lockAcquired = false;
            for (MessageQueueEntry entry : result) {
                // Stale lock so we can discard it
                if (entry.getBodyAsLong() < curTimeMicros) {
//                    stats.incExpiredLockCount();
                    entityManager.remove(entry);
                } else if (entry.getState() == MessageQueueEntryState.Acquired) {
                    throw new BusyLockException("Not first lock");
                } else {
                    lockCount++;
                    if (lockCount == 1 && entry.getTimestamp().equals(lockColumn.getTimestamp())) {
                        lockAcquired = true;
                    }
                }
                if (!lockAcquired) {
                    throw new BusyLockException("Not first lock");
                }
                
                // Write the acquired lock column
                lockColumn.setBodyFromLong(curTimeMicros + this.lockTimeout);
                lockColumn.setTtl(this.lockTtl);
                
                entityManager.put(lockColumn);
            }
            shardLock = new CassandraShardLock(lockColumn, result.size());
            return shardLock;
        } catch (BusyLockException e) {
            throw e;
        } catch (ConnectionException e) {
            LOG.error("Error reading shard " + shardName, e);
            throw new MessageQueueException("Error", e);
        } catch (Exception e) {
            LOG.error("Error reading shard " + shardName, e);
            throw new MessageQueueException("Error", e);
        } finally {
            if (shardLock == null)
                entityManager.commit();
        }
    }

    @Override
    public void releaseLock(ShardLock lock) throws MessageQueueException {
        if (lock != null) {
            CassandraShardLock cassLock = (CassandraShardLock)lock;
            entityManager.remove(cassLock.entry);
            entityManager.commit();
        }
    }

}
