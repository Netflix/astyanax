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

import java.util.Collection;
import java.util.Iterator;
import java.util.HashMap;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.AbstractColumnList;
import com.netflix.astyanax.model.Column;

/**
 * Wrapper for a simple list of columns where each column has a scalar value.
 * 
 * @author elandau
 * 
 * @param <C>
 */
public class ThriftColumnListImpl<C> extends AbstractColumnList<C> {
    private final List<org.apache.cassandra.thrift.Column> columns;
    private HashMap<C, org.apache.cassandra.thrift.Column> lookup;
    private final Serializer<C> colSer;

    public ThriftColumnListImpl(List<org.apache.cassandra.thrift.Column> columns, Serializer<C> colSer) {
        Preconditions.checkArgument(columns != null, "Columns must not be null");
        Preconditions.checkArgument(colSer != null, "Serializer must not be null");

        this.colSer = colSer;
        this.columns = columns;
    }

    @Override
    public Iterator<Column<C>> iterator() {
        class IteratorImpl implements Iterator<Column<C>> {
            Iterator<org.apache.cassandra.thrift.Column> base;

            public IteratorImpl(Iterator<org.apache.cassandra.thrift.Column> base) {
                this.base = base;
            }

            @Override
            public boolean hasNext() {
                return base.hasNext();
            }

            @Override
            public Column<C> next() {
                org.apache.cassandra.thrift.Column c = base.next();
                return new ThriftColumnImpl<C>(colSer.fromBytes(c.getName()), c);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Iterator is immutable");
            }
        }
        return new IteratorImpl(columns.iterator());
    }

    @Override
    public Column<C> getColumnByName(C columnName) {
        constructColumnMap();
        org.apache.cassandra.thrift.Column c = lookup.get(columnName);
        if (c == null) {
            return null;
        }
        return new ThriftColumnImpl<C>(colSer.fromBytes(c.getName()), c);
    }
    
    private void constructColumnMap() {
        if (lookup == null) {
            lookup = Maps.newHashMap();;
            for (org.apache.cassandra.thrift.Column column : columns) {
                lookup.put(colSer.fromBytes(column.getName()), column);
            }
        }
    }

    @Override
    public Column<C> getColumnByIndex(int idx) {
        org.apache.cassandra.thrift.Column c = columns.get(idx);
        return new ThriftColumnImpl<C>(colSer.fromBytes(c.getName()), c);
    }

    public C getNameByIndex(int idx) {
        org.apache.cassandra.thrift.Column column = columns.get(idx);
        return colSer.fromBytes(column.getName());
    }

    @Override
    public <C2> Column<C2> getSuperColumn(C columnName, Serializer<C2> colSer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <C2> Column<C2> getSuperColumn(int idx, Serializer<C2> colSer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        return columns.isEmpty();
    }

    @Override
    public int size() {
        return columns.size();
    }

    @Override
    public boolean isSuperColumn() {
        return false;
    }

    @Override
    public Collection<C> getColumnNames() {
        constructColumnMap();
        return lookup.keySet();
    }

}
