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
package com.netflix.astyanax.thrift.model;

import java.util.List;

import com.netflix.astyanax.AbstractColumnListMutation;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.ColumnPath;
import com.netflix.astyanax.serializers.UUIDSerializer;

public class ThriftCounterSuperColumnMutationImpl<C> extends AbstractColumnListMutation<C> {
    private final List<Mutation> mutationList;
    private final ColumnPath<C> path;
    private CounterSuperColumn superColumn;
    private SlicePredicate deletionPredicate;

    public ThriftCounterSuperColumnMutationImpl(long timestamp, List<Mutation> mutationList, ColumnPath<C> path) {
        super(timestamp);
        this.path = path;
        this.mutationList = mutationList;
    }

    @Override
    public <V> ColumnListMutation<C> putColumn(C columnName, V value, Serializer<V> valueSerializer, Integer ttl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnListMutation<C> putEmptyColumn(C columnName, Integer ttl) {
        return putColumn(columnName, null, UUIDSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putEmptyColumn(final C columnName) {
        return putEmptyColumn(columnName, null);
    }

    @Override
    public ColumnListMutation<C> delete() {
        // Delete the entire super column
        Deletion d = new Deletion().setSuper_column(path.get(0)).setTimestamp(timestamp);
        mutationList.add(new Mutation().setDeletion(d));
        timestamp++;
        return this;
    }

    @Override
    public <SC> ColumnListMutation<SC> withSuperColumn(ColumnPath<SC> superColumnPath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnListMutation<C> incrementCounterColumn(C columnName, long amount) {
        // 1. Set up the column with all the data
        CounterColumn column = new CounterColumn();
        column.setName(path.getSerializer().toByteBuffer(columnName));
        column.setValue(amount);

        // 2. Create the super column mutation if this is the first call
        if (superColumn == null) {
            superColumn = new CounterSuperColumn().setName(path.get(0));

            Mutation mutation = new Mutation();
            mutation.setColumn_or_supercolumn(new ColumnOrSuperColumn().setCounter_super_column(superColumn));
            mutationList.add(mutation);
        }
        superColumn.addToColumns(column);

        return this;
    }

    @Override
    public ColumnListMutation<C> deleteColumn(C columnName) {
        if (deletionPredicate == null) {
            deletionPredicate = new SlicePredicate();
            Deletion d = new Deletion().setTimestamp(timestamp).setSuper_column(path.get(0))
                    .setPredicate(deletionPredicate);

            mutationList.add(new Mutation().setDeletion(d));
        }

        deletionPredicate.addToColumn_names(path.getSerializer().toByteBuffer(columnName));
        return this;
    }

    @Override
    public ColumnListMutation<C> setDefaultTtl(Integer ttl) {
        // TODO: Throw an exception
        return this;
    }
}
