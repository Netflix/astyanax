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
package com.netflix.astyanax;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import com.netflix.astyanax.model.ColumnPath;

/**
 * Abstraction for batching column operations on a single row.
 * 
 * @author elandau
 * 
 * @param <C>
 */
public interface ColumnListMutation<C> {
    /**
     * Generic call to insert a column value with a custom serializer. User this
     * only when you need a custom serializer otherwise use the overloaded
     * putColumn calls to insert common value types.
     * 
     * @param <V>
     * @param columnName
     * @param value
     * @param valueSerializer
     * @param ttl
     * @return
     */
    <V> ColumnListMutation<C> putColumn(C columnName, V value, Serializer<V> valueSerializer, Integer ttl);
    <V> ColumnListMutation<C> putColumnIfNotNull(C columnName, V value, Serializer<V> valueSerializer, Integer ttl);

    /**
     * @deprecated Super columns are being phased out. Use composite columns
     *             instead.
     */
    <SC> ColumnListMutation<SC> withSuperColumn(ColumnPath<SC> superColumnPath);

    ColumnListMutation<C> putColumn(C columnName, String value, Integer ttl);
    ColumnListMutation<C> putColumn(C columnName, String value);
    ColumnListMutation<C> putColumnIfNotNull(C columnName, String value, Integer ttl);
    ColumnListMutation<C> putColumnIfNotNull(C columnName, String value);

    ColumnListMutation<C> putColumn(C columnName, byte[] value, Integer ttl);
    ColumnListMutation<C> putColumn(C columnName, byte[] value);
    ColumnListMutation<C> putColumnIfNotNull(C columnName, byte[] value, Integer ttl);
    ColumnListMutation<C> putColumnIfNotNull(C columnName, byte[] value);

    ColumnListMutation<C> putColumn(C columnName, int value, Integer ttl);
    ColumnListMutation<C> putColumn(C columnName, int value);
    ColumnListMutation<C> putColumnIfNotNull(C columnName, Integer value, Integer ttl);
    ColumnListMutation<C> putColumnIfNotNull(C columnName, Integer value);

    ColumnListMutation<C> putColumn(C columnName, long value, Integer ttl);
    ColumnListMutation<C> putColumn(C columnName, long value);
    ColumnListMutation<C> putColumnIfNotNull(C columnName, Long value, Integer ttl);
    ColumnListMutation<C> putColumnIfNotNull(C columnName, Long value);

    ColumnListMutation<C> putColumn(C columnName, boolean value, Integer ttl);
    ColumnListMutation<C> putColumn(C columnName, boolean value);
    ColumnListMutation<C> putColumnIfNotNull(C columnName, Boolean value, Integer ttl);
    ColumnListMutation<C> putColumnIfNotNull(C columnName, Boolean value);

    ColumnListMutation<C> putColumn(C columnName, ByteBuffer value, Integer ttl);
    ColumnListMutation<C> putColumn(C columnName, ByteBuffer value);
    ColumnListMutation<C> putColumnIfNotNull(C columnName, ByteBuffer value, Integer ttl);
    ColumnListMutation<C> putColumnIfNotNull(C columnName, ByteBuffer value);

    ColumnListMutation<C> putColumn(C columnName, Date value, Integer ttl);
    ColumnListMutation<C> putColumn(C columnName, Date value);
    ColumnListMutation<C> putColumnIfNotNull(C columnName, Date value, Integer ttl);
    ColumnListMutation<C> putColumnIfNotNull(C columnName, Date value);

    ColumnListMutation<C> putColumn(C columnName, float value, Integer ttl);
    ColumnListMutation<C> putColumn(C columnName, float value);
    ColumnListMutation<C> putColumnIfNotNull(C columnName, Float value, Integer ttl);
    ColumnListMutation<C> putColumnIfNotNull(C columnName, Float value);

    ColumnListMutation<C> putColumn(C columnName, double value, Integer ttl);
    ColumnListMutation<C> putColumn(C columnName, double value);
    ColumnListMutation<C> putColumnIfNotNull(C columnName, Double value, Integer ttl);
    ColumnListMutation<C> putColumnIfNotNull(C columnName, Double value);

    ColumnListMutation<C> putColumn(C columnName, UUID value, Integer ttl);
    ColumnListMutation<C> putColumn(C columnName, UUID value);
    ColumnListMutation<C> putColumnIfNotNull(C columnName, UUID value, Integer ttl);
    ColumnListMutation<C> putColumnIfNotNull(C columnName, UUID value);

    ColumnListMutation<C> putEmptyColumn(C columnName, Integer ttl);
    ColumnListMutation<C> putEmptyColumn(C columnName);

    ColumnListMutation<C> incrementCounterColumn(C columnName, long amount);

    ColumnListMutation<C> deleteColumn(C columnName);

    /**
     * The the timestamp for all subsequent column operation in this ColumnListMutation
     * This timestamp does not affect the current timestamp for the entire MutationBatch
     * @param timestamp New timestamp in microseconds
     * @return
     */
    ColumnListMutation<C> setTimestamp(long timestamp);

    /**
     * Deletes all columns at the current column path location. Delete at the
     * root of a row effectively deletes the entire row. This operation also
     * increments the internal timestamp by 1 so new mutations can be added to
     * this row.
     * 
     * @return
     */
    ColumnListMutation<C> delete();

    /**
     * Set the default TTL to use when null is specified to a column insert. The
     * default TTL is null, which means no TTL.
     * 
     * @param ttl  Timeout value in seconds
     * @return
     */
    ColumnListMutation<C> setDefaultTtl(Integer ttl);

}
