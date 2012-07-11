package com.netflix.astyanax.recipes.uniqueness;

import java.util.List;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.locks.BusyLockException;
import com.netflix.astyanax.recipes.locks.StaleLockException;

/**
 * Multi-row uniqueness constraint where all involved column families are dedicated
 * only to uniquness constraint.
 * @author elandau
 *
 * @param <C>
 */
public class DedicatedMultiRowUniquenessConstraint<C> implements UniquenessConstraint {

    private final Keyspace keyspace;

    private Integer ttl = null;
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.CL_LOCAL_QUORUM;
    private final C uniqueColumnName;
    
    private class Row<K> {
        private final ColumnFamily<K, C> columnFamily;
        private final K row;
        
        Row(ColumnFamily<K, C> columnFamily, K row) {
            super();
            this.columnFamily = columnFamily;
            this.row = row;
        }
        
        void fillMutation(MutationBatch m, Integer ttl) {
            m.withRow(columnFamily, row).putEmptyColumn(uniqueColumnName, ttl);
        }
        
        void fillReleaseMutation(MutationBatch m) {
            m.withRow(columnFamily, row).deleteColumn(uniqueColumnName);
        }
        
        void verifyLock() throws Exception {
            // Phase 2: Read back all columns. There should be only 1
            ColumnList<C> result = keyspace.prepareQuery(columnFamily).setConsistencyLevel(consistencyLevel)
                    .getKey(row).execute().getResult();

            if (result.size() != 1) {
                throw new NotUniqueException(row.toString());
            }
        }
    }
    
    private final List<Row<?>> locks = Lists.newArrayList();

    public DedicatedMultiRowUniquenessConstraint(Keyspace keyspace, Supplier<C> uniqueColumnSupplier) {
        this.keyspace = keyspace;
        this.uniqueColumnName = uniqueColumnSupplier.get();
    }

    public DedicatedMultiRowUniquenessConstraint(Keyspace keyspace, C uniqueColumnName) {
        this.keyspace = keyspace;
        this.uniqueColumnName = uniqueColumnName;
    }

    /**
     * TTL to use for the uniquness operation. This is the TTL for the columns
     * to expire in the event of a client crash before the uniqueness can be
     * committed
     * 
     * @param ttl
     * @return
     */
    public DedicatedMultiRowUniquenessConstraint<C> withTtl(Integer ttl) {
        this.ttl = ttl;
        return this;
    }

    /**
     * Consistency level used
     * 
     * @param consistencyLevel
     * @return
     */
    public DedicatedMultiRowUniquenessConstraint<C> withConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    /**
     * Add a row to the set of rows being tested for uniqueness
     * 
     * @param columnFamily
     * @param rowKey
     * @return
     */
    public <K> DedicatedMultiRowUniquenessConstraint<C> withRow(ColumnFamily<K, C> columnFamily, K rowKey) {
        locks.add(new Row<K>(columnFamily, rowKey));
        return this;
    }
    
    public C getLockColumn() {
        return uniqueColumnName;
    }

    @Override
    public void acquire() throws NotUniqueException, Exception {
        acquireAndMutate(null);
    }
    
    @Override
    public void acquireAndMutate(MutationBatch other) throws NotUniqueException, Exception {
        // Insert lock check column for all rows in a single batch mutation
        try {
            MutationBatch m = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
            for (Row<?> lock : locks) {
                lock.fillMutation(m, ttl);
            }
            m.execute();

            // Check each lock in order
            for (Row<?> lock : locks) {
                lock.verifyLock();
            }

            // Commit the unique columns
            for (Row<?> lock : locks) {
                lock.fillMutation(m, null);
            }
            
            if (other != null) {
                m.mergeShallow(other);
            }
            m.execute();
        }
        catch (BusyLockException e) {
            release();
            throw new NotUniqueException(e);
        }
        catch (StaleLockException e) {
            release();
            throw new NotUniqueException(e);
        }
        catch (Exception e) {
            release();
            throw e;
        }
    }

    @Override
    public void release() throws Exception {
        MutationBatch m = keyspace.prepareMutationBatch();
        for (Row<?> lock : locks) {
            lock.fillReleaseMutation(m);
        }
        m.execute();
    }
}
