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
import com.netflix.astyanax.util.RangeBuilder;

@Deprecated
public class UniquenessConstraintWithPrefix<K> {
    private final ColumnFamily<K, String> columnFamily;
    private final Keyspace keyspace;
    private String prefix;
    private Supplier<String> uniqueColumnSupplier = UUIDStringSupplier.getInstance();
    private Integer ttl;
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.CL_QUORUM;
    private UniquenessConstraintViolationMonitor<K, String> monitor;

    public UniquenessConstraintWithPrefix(Keyspace keyspace, ColumnFamily<K, String> columnFamily) {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
    }

    public UniquenessConstraintWithPrefix<K> setColumnNameSupplier(Supplier<String> uniqueColumnSupplier) {
        this.uniqueColumnSupplier = uniqueColumnSupplier;
        return this;
    }

    public UniquenessConstraintWithPrefix<K> setPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    public UniquenessConstraintWithPrefix<K> setTtl(Integer ttl) {
        this.ttl = ttl;
        return this;
    }

    public UniquenessConstraintWithPrefix<K> setMonitor(UniquenessConstraintViolationMonitor<K, String> monitor) {
        this.monitor = monitor;
        return this;
    }

    public UniquenessConstraintWithPrefix<K> setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    public String isUnique(K key) throws ConnectionException {
        String unique = uniqueColumnSupplier.get();

        // Phase 1: Write a unique column
        MutationBatch m = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
        m.withRow(columnFamily, key).putEmptyColumn(prefix + unique, ttl);

        m.execute();

        // Phase 2: Read back all columns. There should be only 1
        ColumnList<String> result = keyspace.prepareQuery(columnFamily).setConsistencyLevel(consistencyLevel)
                .getKey(key)
                .withColumnRange(new RangeBuilder().setStart(prefix + "\u0000").setEnd(prefix + "\uFFFF").build())
                .execute().getResult();

        if (result.size() == 1) {
            return prefix + unique;
        }

        if (this.monitor != null)
            this.monitor.onViolation(key, prefix + unique);

        // Rollback
        m = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
        m.withRow(columnFamily, key).deleteColumn(prefix + unique);
        m.execute().getResult();

        return null;
    }
}
