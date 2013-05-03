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
import java.util.List;

import com.google.common.base.Preconditions;
import com.netflix.astyanax.AbstractColumnListMutation;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.ColumnPath;

/**
 * Implementation of a row mutation at the root of the column family.
 * 
 * @author elandau
 * 
 * @param <C>
 */
public class ThriftColumnFamilyMutationImpl<C> extends AbstractColumnListMutation<C> {
    private final Serializer<C> columnSerializer;
    private final List<Mutation> mutationList;
    private Deletion lastDeletion;
    
    public ThriftColumnFamilyMutationImpl(Long timestamp, List<Mutation> mutationList, Serializer<C> columnSerializer) {
        super(timestamp);
        this.mutationList = mutationList;
        this.columnSerializer = columnSerializer;
    }

    @Override
    public <SC> ColumnListMutation<SC> withSuperColumn(ColumnPath<SC> superColumnPath) {
        return new ThriftSuperColumnMutationImpl<SC>(timestamp, mutationList, superColumnPath);
    }

    @Override
    public <V> ColumnListMutation<C> putColumn(C columnName, V value, Serializer<V> valueSerializer, Integer ttl) {
        Preconditions.checkNotNull(columnName, "Column name cannot be null");
        
        // 1. Set up the column with all the data
        Column column = new Column();
        column.setName(columnSerializer.toByteBuffer(columnName));
        if (column.getName().length == 0) {
            throw new RuntimeException("Column name cannot be empty");
        }
        
        if (value == null) 
            column.setValue(ThriftUtils.EMPTY_BYTE_BUFFER);
        else 
            column.setValue(valueSerializer.toByteBuffer(value));
        
        column.setTimestamp(timestamp);
        if (ttl != null) {
            // Treat TTL of 0 or -1 as no TTL
            if (ttl > 0)
                column.setTtl(ttl);
        }
        else if (defaultTtl != null)
            column.setTtl(defaultTtl);

        // 2. Create a mutation and append to the mutation list.
        Mutation mutation = new Mutation();
        mutation.setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(column));
        mutationList.add(mutation);

        return this;
    }

    @Override
    public ColumnListMutation<C> putEmptyColumn(C columnName, Integer ttl) {
        Preconditions.checkNotNull(columnName, "Column name cannot be null");
        
        Column column = new Column();
        column.setName(columnSerializer.toByteBuffer(columnName));
        if (column.getName().length == 0) {
            throw new RuntimeException("Column name cannot be empty");
        }
        
        column.setValue(ThriftUtils.EMPTY_BYTE_BUFFER);
        column.setTimestamp(timestamp);
        if (ttl != null) {
            // Treat TTL of 0 or -1 as no TTL
            if (ttl > 0)
                column.setTtl(ttl);
        }
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
        Preconditions.checkNotNull(columnName, "Column name cannot be null");
        
        // 1. Set up the column with all the data
        CounterColumn column = new CounterColumn();
        column.setName(columnSerializer.toByteBuffer(columnName));
        if (column.getName().length == 0) {
            throw new RuntimeException("Column name cannot be empty");
        }
        column.setValue(amount);

        // 2. Create a mutation and append to the mutation list.
        Mutation mutation = new Mutation();
        mutation.setColumn_or_supercolumn(new ColumnOrSuperColumn().setCounter_column(column));
        mutationList.add(mutation);
        return this;
    }

    @Override
    public ColumnListMutation<C> deleteColumn(C columnName) {
        Preconditions.checkNotNull(columnName, "Column name cannot be null");
        
        // Create a reusable predicate for deleting columns and insert only once
        if (null == lastDeletion || lastDeletion.getTimestamp() != timestamp) {
            lastDeletion = new Deletion().setPredicate(new SlicePredicate()).setTimestamp(timestamp);
            mutationList.add(new Mutation().setDeletion(lastDeletion));
        }
        
        ByteBuffer bb = this.columnSerializer.toByteBuffer(columnName);
        if (!bb.hasRemaining()) {
            throw new RuntimeException("Column name cannot be empty");
        }

        lastDeletion.getPredicate().addToColumn_names(bb);
        return this;
    }

}
