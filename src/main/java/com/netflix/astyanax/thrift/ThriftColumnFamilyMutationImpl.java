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
package com.netflix.astyanax.thrift;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.ColumnPath;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.DateSerializer;
import com.netflix.astyanax.serializers.DoubleSerializer;
import com.netflix.astyanax.serializers.FloatSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;

/**
 * Implementation of a row mutation at the root of the column family.
 * 
 * @author elandau
 * 
 * @param <C>
 */
public class ThriftColumnFamilyMutationImpl<C> implements ColumnListMutation<C> {
    private final Serializer<C> columnSerializer;
    private final List<Mutation> mutationList;
    private long timestamp;
    private SlicePredicate deletionPredicate;
    private Integer defaultTtl = null;

    public ThriftColumnFamilyMutationImpl(Long timestamp, List<Mutation> mutationList, Serializer<C> columnSerializer) {
        this.mutationList = mutationList;
        this.columnSerializer = columnSerializer;
        this.timestamp = timestamp;
    }

    @Override
    public <SC> ColumnListMutation<SC> withSuperColumn(ColumnPath<SC> superColumnPath) {
        return new ThriftSuperColumnMutationImpl<SC>(timestamp, mutationList, superColumnPath);
    }

    @Override
    public <V> ColumnListMutation<C> putColumn(C columnName, V value, Serializer<V> valueSerializer, Integer ttl) {
        // 1. Set up the column with all the data
        Column column = new Column();
        column.setName(columnSerializer.toByteBuffer(columnName));
        column.setValue(valueSerializer.toByteBuffer(value));
        column.setTimestamp(timestamp);
        if (ttl != null)
            column.setTtl(ttl);
        else if (defaultTtl != null)
            column.setTtl(defaultTtl);

        // 2. Create a mutation and append to the mutation list.
        Mutation mutation = new Mutation();
        mutation.setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(column));
        mutationList.add(mutation);

        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, String value, Integer ttl) {
        return putColumn(columnName, value, StringSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, byte[] value, Integer ttl) {
        return putColumn(columnName, value, BytesArraySerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, int value, Integer ttl) {
        return putColumn(columnName, value, IntegerSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, long value, Integer ttl) {
        return putColumn(columnName, value, LongSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, boolean value, Integer ttl) {
        return putColumn(columnName, value, BooleanSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, ByteBuffer value, Integer ttl) {
        return putColumn(columnName, value, ByteBufferSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, Date value, Integer ttl) {
        return putColumn(columnName, value, DateSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, float value, Integer ttl) {
        return putColumn(columnName, value, FloatSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, double value, Integer ttl) {
        return putColumn(columnName, value, DoubleSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, UUID value, Integer ttl) {
        return putColumn(columnName, value, UUIDSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putEmptyColumn(C columnName, Integer ttl) {
        Column column = new Column();
        column.setName(columnSerializer.toByteBuffer(columnName));
        column.setValue(ThriftUtils.EMPTY_BYTE_BUFFER);
        column.setTimestamp(timestamp);
        if (ttl != null)
            column.setTtl(ttl);
        else if (defaultTtl != null)
            column.setTtl(defaultTtl);

        // 2. Create a mutation and append to the mutation list.
        Mutation mutation = new Mutation();
        mutation.setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(column));
        mutationList.add(mutation);
        return this;
    }

    @Override
    public ColumnListMutation<C> delete() {
        // Delete the entire row
        Deletion d = new Deletion().setTimestamp(timestamp);
        mutationList.add(new Mutation().setDeletion(d));

        // Increment the timestamp by 1 so subsequent puts on this column may be
        // written
        timestamp++;
        return this;
    }

    @Override
    public ColumnListMutation<C> incrementCounterColumn(C columnName, long amount) {
        // 1. Set up the column with all the data
        CounterColumn column = new CounterColumn();
        column.setName(columnSerializer.toByteBuffer(columnName));
        column.setValue(amount);

        // 2. Create a mutation and append to the mutation list.
        Mutation mutation = new Mutation();
        mutation.setColumn_or_supercolumn(new ColumnOrSuperColumn().setCounter_column(column));
        mutationList.add(mutation);
        return this;
    }

    @Override
    public ColumnListMutation<C> deleteColumn(C columnName) {
        // Create a reusable predicate for deleting columns and insert only once
        if (null == deletionPredicate) {
            deletionPredicate = new SlicePredicate();
            Deletion d = new Deletion().setPredicate(deletionPredicate).setTimestamp(timestamp);
            mutationList.add(new Mutation().setDeletion(d));
        }
        ByteBuffer bb = this.columnSerializer.toByteBuffer(columnName);
        deletionPredicate.addToColumn_names(bb);
        return this;
    }

    @Override
    public ColumnListMutation<C> setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    @Override
    public ColumnListMutation<C> setDefaultTtl(Integer ttl) {
        this.defaultTtl = ttl;
        return this;
    }
}
