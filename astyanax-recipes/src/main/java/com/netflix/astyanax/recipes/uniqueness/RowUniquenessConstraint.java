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

import java.nio.ByteBuffer;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * Test uniqueness for a single row.  This implementation allows for any 
 * column type.  If the column family uses UTF8Type for the comparator
 * then it is preferable to use ColumnPrefixUniquenessConstraint.
 * 
 * @author elandau
 * 
 * @param <K>
 * @param <C>
 */
public class RowUniquenessConstraint<K, C> implements UniquenessConstraint {
    private final ColumnFamily<K, C> columnFamily;
    private final Keyspace   keyspace;
    private final C          uniqueColumn;
    private final K          key;
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.CL_LOCAL_QUORUM;
    private ByteBuffer       data             = null;
    private Integer          ttl              = null;

    public RowUniquenessConstraint(Keyspace keyspace, ColumnFamily<K, C> columnFamily, K key,
            Supplier<C> uniqueColumnSupplier) {
        this.keyspace     = keyspace;
        this.columnFamily = columnFamily;
        this.uniqueColumn = uniqueColumnSupplier.get();
        this.key          = key;
    }

    public RowUniquenessConstraint<K, C> withTtl(Integer ttl) {
        this.ttl = ttl;
        return this;
    }

    public RowUniquenessConstraint<K, C> withConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }
    
    /**
     * Specify the data value to add to the column.  
     * @param data
     * @return
     */
    public RowUniquenessConstraint<K, C> withData(ByteBuffer data) {
        this.data = data;
        return this;
    }
    
    public RowUniquenessConstraint<K, C> withData(String data) {
        this.data = StringSerializer.get().fromString(data);
        return this;
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
    public void acquireAndMutate(final MutationBatch mutation) throws NotUniqueException, Exception {
        acquireAndApplyMutation(new Function<MutationBatch, Boolean>() {
            @Override
            public Boolean apply(MutationBatch input) {
                if (mutation != null)
                    input.mergeShallow(mutation);
                return true;
            }
        });
    }
    
    @Override
    public void acquireAndApplyMutation(Function<MutationBatch, Boolean> callback) throws NotUniqueException, Exception {
        try {
            // Phase 1: Write a unique column
            MutationBatch m = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
            if (data == null) {
                m.withRow(columnFamily, key).putEmptyColumn(uniqueColumn, ttl);
            }
            else {
                m.withRow(columnFamily, key).putColumn(uniqueColumn, data, ttl);
            }
            m.execute();

            // Phase 2: Read back all columns. There should be only 1
            ColumnList<C> result = keyspace.prepareQuery(columnFamily).setConsistencyLevel(consistencyLevel)
                    .getKey(key).execute().getResult();

            if (result.size() != 1) {
                throw new NotUniqueException(key.toString());
            }

            // Phase 3: Persist the uniqueness with 
            m = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
            if (callback != null)
                callback.apply(m);
            
            if (data == null) {
                m.withRow(columnFamily, key).putEmptyColumn(uniqueColumn, null);
            }
            else {
                m.withRow(columnFamily, key).putColumn(uniqueColumn, data, null);
            }
            m.execute();
        }
        catch (Exception e) {
            release();
            throw e;
        }    }

    @Override
    public void release() throws Exception {
        MutationBatch m = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
        m.withRow(columnFamily, key).deleteColumn(uniqueColumn);
        m.execute();
    }
    
    /**
     * Read the data stored with the unique row.  This data is normally a 'foreign' key to
     * another column family.
     * @return
     * @throws Exception
     */
    public ByteBuffer readData() throws Exception {
        ColumnList<C> result = keyspace
                .prepareQuery(columnFamily)
                    .setConsistencyLevel(consistencyLevel)
                    .getKey(key)
                .execute()
                    .getResult();
        
        boolean hasColumn = false;
        ByteBuffer data = null;
        for (Column<C> column : result) {
            if (column.getTtl() == 0) {
                if (hasColumn) {
                    throw new IllegalStateException("Row has multiple uniquneness locks");
                }
                hasColumn = true;
                data = column.getByteBufferValue();
            }
        }
        
        if (!hasColumn) {
            throw new NotFoundException(this.key.toString() + " has no uniquness lock");
        }
        return data;
    }
    
    public String readDataAsString() throws Exception {
        return StringSerializer.get().fromByteBuffer(readData());
    }
}
