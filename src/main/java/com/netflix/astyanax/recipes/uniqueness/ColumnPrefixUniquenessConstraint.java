package com.netflix.astyanax.recipes.uniqueness;

import java.util.Map.Entry;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;

/**
 * Perform a uniqueness constraint using the locking recipe.  The usage here is to
 * take the lock and then re-write the column without a TTL to 'persist' it in cassandra.
 *
 * @author elandau
 *
 * @param <K>
 */
public class ColumnPrefixUniquenessConstraint<K> implements UniquenessConstraint {

    private final ColumnPrefixDistributedRowLock<K> lock;

    public ColumnPrefixUniquenessConstraint(Keyspace keyspace, ColumnFamily<K, String> columnFamily, K key) {
        lock = new ColumnPrefixDistributedRowLock<K>(keyspace, columnFamily, key);
    }

    public ColumnPrefixUniquenessConstraint<K> withTtl(Integer ttl) {
        lock.withTtl(ttl);
        return this;
    }

    public ColumnPrefixUniquenessConstraint<K> withConsistencyLevel(ConsistencyLevel consistencyLevel) {
        lock.withConsistencyLevel(consistencyLevel);
        return this;
    }

    public ColumnPrefixUniquenessConstraint<K> withPrefix(String prefix) {
        lock.withColumnPrefix(prefix);
        return this;
    }

    /**
     * Specify the unique value to use for the column name when doing the uniqueness
     * constraint.  In many cases this will be a TimeUUID that is used as the row
     * key to store the actual data for the unique key tracked in this column
     * family.
     *
     * @param unique
     * @return
     */
    public ColumnPrefixUniquenessConstraint<K> withUniqueId(String unique) {
        lock.withLockId(unique);
        return this;
    }

    public String readUniqueColumn() throws Exception {
        String column = null;
        for (Entry<String, Long> entry : lock.readLockColumns().entrySet()) {
            if (entry.getValue() == 0) {
                if (column == null) {
                    column = entry.getKey().substring(lock.getPrefix().length());
                }
                else {
                    throw new IllegalStateException("Key has multiple locks");
                }
            }
        }

        if (column == null)
            throw new NotFoundException("Unique column not found for " + lock.getKey());
        return column;

    }

    @Override
    public void acquire() throws NotUniqueException, Exception {
        acquireAndMutate(lock.getKeyspace().prepareMutationBatch());
    }


    @Override
    public void acquireAndMutate(MutationBatch m) throws NotUniqueException, Exception {
        lock.acquire();
        m.lockCurrentTimestamp();
        lock.fillReleaseMutation(m, true);
        lock.fillLockMutation(m, null, null);
        m.setConsistencyLevel(lock.getConsistencyLevel())
            .execute();
    }


    @Override
    public void release() throws Exception {
        lock.release();
    }
}
