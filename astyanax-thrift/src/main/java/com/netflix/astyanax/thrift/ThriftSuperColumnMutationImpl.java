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

import java.util.List;

import com.netflix.astyanax.AbstractColumnListMutation;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SuperColumn;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.ColumnPath;

/**
 * @deprecated Use composite columns instead
 * @author elandau
 * 
 * @param <C>
 */
public class ThriftSuperColumnMutationImpl<C> extends AbstractColumnListMutation<C> {
    private final List<Mutation> mutationList;
    private final ColumnPath<C> path;
    private SuperColumn superColumn;
    private SlicePredicate deletionPredicate;

    public ThriftSuperColumnMutationImpl(long timestamp, List<Mutation> mutationList, ColumnPath<C> path) {
        super(timestamp);
        this.path = path;
        this.mutationList = mutationList;
    }

    @Override
    public <V> ColumnListMutation<C> putColumn(C columnName, V value, Serializer<V> valueSerializer, Integer ttl) {
        Column column = new Column();
        column.setName(path.getSerializer().toByteBuffer(columnName));
        column.setValue(valueSerializer.toByteBuffer(value));
        column.setTimestamp(timestamp);
        if (ttl != null)
            column.setTtl(ttl);
        else if (defaultTtl != null)
            column.setTtl(defaultTtl);

        addMutation(column);
        return this;
    }

    private void addMutation(Column column) {
        // 2. Create the super column mutation if this is the first call
        if (superColumn == null) {
            superColumn = new SuperColumn().setName(path.get(0));

            Mutation mutation = new Mutation();
            mutation.setColumn_or_supercolumn(new ColumnOrSuperColumn().setSuper_column(superColumn));
            mutationList.add(mutation);
        }

        superColumn.addToColumns(column);
    }

    @Override
    public ColumnListMutation<C> putEmptyColumn(C columnName, Integer ttl) {
        Column column = new Column();
        column.setName(path.getSerializer().toByteBuffer(columnName));
        column.setValue(ThriftUtils.EMPTY_BYTE_BUFFER);
        column.setTimestamp(timestamp);
        if (ttl != null)
            column.setTtl(ttl);
        else if (defaultTtl != null)
            column.setTtl(defaultTtl);

        addMutation(column);
        return this;
    }

    @Override
    public ColumnListMutation<C> delete() {
        Deletion d = new Deletion();
        d.setSuper_column(path.get(0));
        d.setTimestamp(timestamp);
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
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnListMutation<C> deleteColumn(C columnName) {
        if (deletionPredicate == null) {
            deletionPredicate = new SlicePredicate();
            Deletion d = new Deletion();
            d.setTimestamp(timestamp);
            d.setSuper_column(path.get(0));
            d.setPredicate(deletionPredicate);

            mutationList.add(new Mutation().setDeletion(d));
        }

        deletionPredicate.addToColumn_names(path.getSerializer().toByteBuffer(columnName));
        return this;
    }

    @Override
    public ColumnListMutation<C> putCompressedColumn(C columnName, String value, Integer ttl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnListMutation<C> putCompressedColumn(C columnName, String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnListMutation<C> putCompressedColumnIfNotNull(C columnName, String value, Integer ttl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnListMutation<C> putCompressedColumnIfNotNull(C columnName, String value) {
        throw new UnsupportedOperationException();
    }

}
