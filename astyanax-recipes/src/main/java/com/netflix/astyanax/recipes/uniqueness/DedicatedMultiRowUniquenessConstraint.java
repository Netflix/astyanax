/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.recipes.uniqueness;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
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
        
        Column<C> getUniqueColumn() throws ConnectionException {
            ColumnList<C> columns = keyspace.prepareQuery(columnFamily).getKey(row).execute().getResult();
            Column<C> foundColumn = null;
            for (Column<C> column : columns) {
                if (column.getTtl() == 0) {
                    if (foundColumn != null)
                        throw new RuntimeException("Row has duplicate quite columns");
                    foundColumn = column;
                }
            }
            if (foundColumn == null) {
                throw new NotFoundException("Unique column not found");
            }
            return foundColumn;
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
        acquireAndApplyMutation(null);
    }
    
    /**
     * @deprecated  Use acquireAndExecuteMutation instead to avoid timestamp issues
     */
    @Override
    @Deprecated
    public void acquireAndMutate(final MutationBatch other) throws NotUniqueException, Exception {
        acquireAndApplyMutation(new Function<MutationBatch, Boolean>() {
            @Override
            public Boolean apply(MutationBatch input) {
                if (other != null) {
                    input.mergeShallow(other);
                }
                return true;
            }
        });
    }
    
    @Override
    public void acquireAndApplyMutation(Function<MutationBatch, Boolean> callback) throws NotUniqueException, Exception {
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
            
            if (callback != null)
                callback.apply(m);
            
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
    
    /**
     * @return
     * @throws Exception
     */
    public Column<C> getUniqueColumn() throws Exception {
        if (locks.size() == 0) 
            throw new IllegalStateException("Missing call to withRow to add rows to the uniqueness constraint");
        
        // Get the unique row from all columns
        List<Column<C>> columns = Lists.newArrayList();
        for (Row<?> row : locks) {
            columns.add(row.getUniqueColumn());
        }
        
        // Check that all rows have the same unique column otherwise they are not part of 
        // the same unique group
        Column<C> foundColumn = columns.get(0);
        for (int i = 1; i < columns.size(); i++) {
            Column<C> nextColumn = columns.get(i);
            if (!nextColumn.getRawName().equals(foundColumn.getRawName())) {
                throw new NotUniqueException("The provided rows are not part of the same uniquness constraint");
            }
            
            if (foundColumn.hasValue() != nextColumn.hasValue()) {
                throw new NotUniqueException("The provided rows are not part of the same uniquness constraint");
            }
            
            if (foundColumn.hasValue() && !nextColumn.getByteBufferValue().equals((foundColumn.getByteBufferValue()))) {
                throw new NotUniqueException("The provided rows are not part of the same uniquness constraint");
            }
        }
        
        return foundColumn;
    }

}
