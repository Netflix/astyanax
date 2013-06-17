package com.netflix.astyanax.recipes.queue.lock;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatchManager;
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

/**
 * Implement a lock using unique column names and a sequence of events.
 * 
 * 1.  Write a UUID column with state=lock attempt.  Value=lock timeout
 * 2.  Read back all columns
 *      a.  If column timeout > now, discard
 *      b.  If column 
 * @author elandau
 *
 */
public class CassandraShardLockManager implements ShardLockManager {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraShardLockManager.class);
    
    private final CompositeEntityManager<MessageQueueEntry, String> entityManager;

    private long  lockTimeout    = TimeUnit.MILLISECONDS.convert(60,  TimeUnit.SECONDS);
    private int   lockTtl        = 120;
    
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
            ConsistencyLevel     consistencyLevel,
            MessageQueueInfo     queueInfo) {
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
            lockColumn = MessageQueueEntry.newLockEntry(shardName, MessageQueueEntryState.None, lockTtl);
            long curTimeMillis = lockColumn.getTimestamp(TimeUnit.MICROSECONDS);
            lockColumn.setBodyFromLong(curTimeMillis + lockTimeout);
            entityManager.put(lockColumn);
            entityManager.commit();
            
            // No matter how the lock succeeds we always want to delete the temp lock column
            entityManager.remove(lockColumn);
            
            // 2. Read back lock columns and entries
            Collection<MessageQueueEntry> locks = getLockColumns(shardName);
            
            boolean lockAcquired = false;
            for (MessageQueueEntry entry : locks) {
                // Our lock is the first in the list ordered by time so assume, for now, that we won the race
                if (entry.equals(lockColumn)) {
                    lockAcquired = true;
                }
                // Stale lock so we can discard it
                else if (entry.getBodyAsLong() > curTimeMillis) {
                }
                // Someone else already acquired the lock.
                else if (entry.getState() == MessageQueueEntryState.Acquired) {
                    throw new BusyLockException("Not first lock");
                } 
                // Someone else has an early lock time
                else if (!lockAcquired) {
                    throw new BusyLockException("Not first lock");
                }
            }
            
            // 3.  'Commit' the lock by changing the state to acquired
            lockColumn.setState(MessageQueueEntryState.Acquired);
            entityManager.put(lockColumn);
            
            // 4.  Double check that we got the lock
            locks = getLockColumns(shardName);
            for (MessageQueueEntry entry : locks) {
                // There is a lock collision
                if (entry.getState() == MessageQueueEntryState.Acquired && !entry.equals(lockColumn)) {
                    entityManager.remove(lockColumn);
                    throw new BusyLockException("Lock collision");
                } 
            }

            shardLock = new CassandraShardLock(lockColumn, locks.size());
            return shardLock;
        } catch (BusyLockException e) {
            throw e;
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
