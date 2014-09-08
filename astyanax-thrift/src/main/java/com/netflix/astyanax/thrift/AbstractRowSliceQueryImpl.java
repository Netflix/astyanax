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
import java.util.Arrays;
import java.util.Collection;

import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.query.RowSliceQuery;

public abstract class AbstractRowSliceQueryImpl<K, C> implements RowSliceQuery<K, C> {
    protected SlicePredicate predicate = new SlicePredicate().setSlice_range(ThriftUtils.createAllInclusiveSliceRange());
    private Serializer<C> serializer;

    public AbstractRowSliceQueryImpl(Serializer<C> serializer) {
        this.serializer = serializer;
    }

    @Override
    public RowSliceQuery<K, C> withColumnSlice(C... columns) {
        if (columns != null) {
            predicate.setColumn_names(serializer.toBytesList(Arrays.asList(columns))).setSlice_rangeIsSet(false);
        }
        return this;
    }

    @Override
    public RowSliceQuery<K, C> withColumnSlice(Collection<C> columns) {
        if (columns != null)
            predicate.setColumn_names(serializer.toBytesList(columns)).setSlice_rangeIsSet(false);
        return this;
    }

    @Override
    public RowSliceQuery<K, C> withColumnRange(C startColumn, C endColumn, boolean reversed, int count) {
        predicate.setSlice_range(ThriftUtils.createSliceRange(serializer, startColumn, endColumn, reversed, count));
        return this;
    }

    @Override
    public RowSliceQuery<K, C> withColumnRange(ByteBuffer startColumn, ByteBuffer endColumn, boolean reversed, int count) {
        predicate.setSlice_range(new SliceRange(startColumn, endColumn, reversed, count));
        return this;
    }

    @Override
    public RowSliceQuery<K, C> withColumnSlice(ColumnSlice<C> slice) {
        if (slice.getColumns() != null) {
            predicate.setColumn_names(serializer.toBytesList(slice.getColumns())).setSlice_rangeIsSet(false);
        }
        else {
            predicate.setSlice_range(ThriftUtils.createSliceRange(serializer, slice.getStartColumn(),
                    slice.getEndColumn(), slice.getReversed(), slice.getLimit()));
        }
        return this;
    }

    @Override
    public RowSliceQuery<K, C> withColumnRange(ByteBufferRange range) {
        predicate.setSlice_range(new SliceRange().setStart(range.getStart()).setFinish(range.getEnd())
                .setCount(range.getLimit()).setReversed(range.isReversed()));
        return this;
    }

}
