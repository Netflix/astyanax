package com.netflix.astyanax.recipes.uniqueness;

import com.google.common.base.Supplier;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * Test uniqueness for a single row.
 * 
 * @author elandau
 * 
 * @param <K>
 * @param <C>
 */
public class RowUniquenessConstraint<K, C> implements UniquenessConstraint {
    private final ColumnFamily<K, C> columnFamily;
    private final Keyspace keyspace;
    private Integer ttl = null;
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.CL_QUORUM;
    private final C uniqueColumn;
    private final K key;

    public RowUniquenessConstraint(Keyspace keyspace,
            ColumnFamily<K, C> columnFamily, K key,
            Supplier<C> uniqueColumnSupplier) {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.uniqueColumn = uniqueColumnSupplier.get();
        this.key = key;
    }

    public RowUniquenessConstraint<K, C> withTtl(Integer ttl) {
        this.ttl = ttl;
        return this;
    }

    public RowUniquenessConstraint<K, C> withConsistencyLevel(
            ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    @Override
    public void acquire() throws NotUniqueException, Exception {
        try {
            // Phase 1: Write a unique column
            MutationBatch m = keyspace.prepareMutationBatch()
                    .setConsistencyLevel(consistencyLevel);
            m.withRow(columnFamily, key).putEmptyColumn(uniqueColumn, ttl);
            m.execute();

            // Phase 2: Read back all columns. There should be only 1
            ColumnList<C> result = keyspace.prepareQuery(columnFamily)
                    .setConsistencyLevel(consistencyLevel).getKey(key)
                    .execute().getResult();

            if (result.size() != 1) {
                throw new NotUniqueException();
            }

            m = keyspace.prepareMutationBatch().setConsistencyLevel(
                    consistencyLevel);
            m.withRow(columnFamily, key).putEmptyColumn(uniqueColumn, null);
            m.execute();
        } catch (Exception e) {
            release();
            throw e;
        }
    }

    @Override
    public void release() throws Exception {
        MutationBatch m = keyspace.prepareMutationBatch().setConsistencyLevel(
                consistencyLevel);
        m.withRow(columnFamily, key).deleteColumn(uniqueColumn);
        m.execute();
    }
}
