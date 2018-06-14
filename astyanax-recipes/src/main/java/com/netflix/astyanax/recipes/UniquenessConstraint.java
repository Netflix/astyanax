/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.astyanax.recipes;

import com.google.common.base.Supplier;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;

@Deprecated
public class UniquenessConstraint<K, C> {
    private final ColumnFamily<K, C> columnFamily;
    private final Keyspace keyspace;
    private final Supplier<C> uniqueColumnSupplier;
    private Integer ttl;
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.CL_QUORUM;
    private UniquenessConstraintViolationMonitor<K, C> monitor;

    public UniquenessConstraint(Keyspace keyspace, ColumnFamily<K, C> columnFamily, Supplier<C> uniqueColumnSupplier) {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.uniqueColumnSupplier = uniqueColumnSupplier;
    }

    public UniquenessConstraint<K, C> setTtl(Integer ttl) {
        this.ttl = ttl;
        return this;
    }

    public UniquenessConstraint<K, C> setMonitor(UniquenessConstraintViolationMonitor<K, C> monitor) {
        this.monitor = monitor;
        return this;
    }

    public C isUnique(K key) throws ConnectionException {
        C unique = uniqueColumnSupplier.get();

        // Phase 1: Write a unique column
        MutationBatch m = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
        m.withRow(columnFamily, key).putEmptyColumn(unique, ttl);
        m.execute();

        // Phase 2: Read back all columns. There should be only 1
        ColumnList<C> result = keyspace.prepareQuery(columnFamily).setConsistencyLevel(consistencyLevel).getKey(key)
                .execute().getResult();

        if (result.size() == 1) {
            return unique;
        }

        if (this.monitor != null)
            this.monitor.onViolation(key, unique);

        // Rollback
        m = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
        m.withRow(columnFamily, key).deleteColumn(unique);
        m.execute();
        return null;
    }
}
