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

import java.util.Map.Entry;

import com.google.common.base.Function;
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
        acquireAndApplyMutation(null);
    }


    /**
     * @deprecated  Use acquireAndExecuteMutation instead to avoid timestamp issues
     */
    @Override
    @Deprecated
    public void acquireAndMutate(MutationBatch m) throws NotUniqueException, Exception {
        lock.acquire();
        m.lockCurrentTimestamp();
        lock.fillReleaseMutation(m, true);
        lock.fillLockMutation(m, null, null);
        m.setConsistencyLevel(lock.getConsistencyLevel())
            .execute();
    }
    
    @Override
    public void acquireAndApplyMutation(Function<MutationBatch, Boolean> callback) throws NotUniqueException, Exception {
        lock.acquire();
        
        MutationBatch mb = lock.getKeyspace().prepareMutationBatch();
        if (callback != null)
            callback.apply(mb);
        lock.fillReleaseMutation(mb,  true);
        lock.fillLockMutation(mb, null, null);
        mb.setConsistencyLevel(lock.getConsistencyLevel())
            .execute();
    }
    
    @Override
    public void release() throws Exception {
        lock.release();
    }
}
