package com.netflix.astyanax.recipes.uniqueness;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;

public class ColumnPrefixUniquenessConstraint<K> implements
        UniquenessConstraint {

    private final ColumnPrefixDistributedRowLock<K> lock;

    public ColumnPrefixUniquenessConstraint(Keyspace keyspace,
            ColumnFamily<K, String> columnFamily, K key) {
        lock = new ColumnPrefixDistributedRowLock<K>(keyspace, columnFamily,
                key);
    }

    public ColumnPrefixUniquenessConstraint<K> withTtl(Integer ttl) {
        lock.withTtl(ttl);
        return this;
    }

    public ColumnPrefixUniquenessConstraint<K> withConsistencyLevel(
            ConsistencyLevel consistencyLevel) {
        lock.withConsistencyLevel(consistencyLevel);
        return this;
    }

    @Override
    public void acquire() throws NotUniqueException, Exception {
        try {
            lock.acquire();

            MutationBatch m = lock.getKeyspace().prepareMutationBatch()
                    .setConsistencyLevel(lock.getConsistencyLevel());
            lock.fillLockMutation(m, null, null);
            m.execute();
        } catch (Exception e) {
            release();
            throw new NotUniqueException(e);
        }
    }

    @Override
    public void release() throws Exception {
        lock.release();
    }
}
