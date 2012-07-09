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
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.SuperColumn;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.AbstractColumnList;
import com.netflix.astyanax.model.Column;

/**
 * List of columns that can be either a list of super columns or standard
 * columns.
 * 
 * @author elandau
 * 
 * @param <C>
 */
public class ThriftColumnOrSuperColumnListImpl<C> extends AbstractColumnList<C> {
    private final List<ColumnOrSuperColumn> columns;
    private Map<C, ColumnOrSuperColumn> lookup;
    private final Serializer<C> colSer;

    public ThriftColumnOrSuperColumnListImpl(List<ColumnOrSuperColumn> columns, Serializer<C> colSer) {
        Preconditions.checkArgument(columns != null, "Columns must not be null");
        Preconditions.checkArgument(colSer != null, "Serializer must not be null");

        this.columns = columns;
        this.colSer = colSer;
    }

    @Override
    public Iterator<Column<C>> iterator() {
        class IteratorImpl implements Iterator<Column<C>> {
            Iterator<ColumnOrSuperColumn> base;

            public IteratorImpl(Iterator<ColumnOrSuperColumn> base) {
                this.base = base;
            }

            @Override
            public boolean hasNext() {
                return base.hasNext();
            }

            @Override
            public Column<C> next() {
                ColumnOrSuperColumn column = base.next();
                if (column.isSetSuper_column()) {
                    SuperColumn sc = column.getSuper_column();
                    return new ThriftSuperColumnImpl<C>(colSer.fromBytes(sc.getName()), sc);
                }
                else if (column.isSetCounter_column()) {
                    CounterColumn cc = column.getCounter_column();
                    return new ThriftCounterColumnImpl<C>(colSer.fromBytes(cc.getName()), cc);
                }
                else if (column.isSetCounter_super_column()) {
                    CounterSuperColumn cc = column.getCounter_super_column();
                    return new ThriftCounterSuperColumnImpl<C>(colSer.fromBytes(cc.getName()), cc);
                }
                else if (column.isSetColumn()) {
                    org.apache.cassandra.thrift.Column c = column.getColumn();
                    return new ThriftColumnImpl<C>(colSer.fromBytes(c.getName()), c);
                }
                else {
                    throw new RuntimeException("Unknwon column type");
                }
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
        ColumnOrSuperColumn column = getColumn(columnName);
        if (column == null) {
            return null;
        }
        else if (column.isSetColumn()) {
            return new ThriftColumnImpl<C>(columnName, column.getColumn());
        }
        else if (column.isSetCounter_column()) {
            return new ThriftCounterColumnImpl<C>(columnName, column.getCounter_column());
        }
        throw new UnsupportedOperationException("SuperColumn " + columnName + " has no value");
    }

    @Override
    public Column<C> getColumnByIndex(int idx) {
        ColumnOrSuperColumn column = columns.get(idx);
        if (column == null) {
            // TODO: throw an exception
            return null;
        }
        else if (column.isSetColumn()) {
            return new ThriftColumnImpl<C>(this.colSer.fromBytes(column.getColumn().getName()), column.getColumn());
        }
        else if (column.isSetCounter_column()) {
            return new ThriftCounterColumnImpl<C>(this.colSer.fromBytes(column.getCounter_column().getName()),
                    column.getCounter_column());
        }
        throw new UnsupportedOperationException("SuperColumn " + idx + " has no value");
    }

    @Override
    public <C2> Column<C2> getSuperColumn(C columnName, Serializer<C2> colSer) {
        ColumnOrSuperColumn column = getColumn(columnName);
        if (column == null) {
            // TODO: throw an exception
            return null;
        }
        else if (column.isSetSuper_column()) {
            SuperColumn sc = column.getSuper_column();
            return new ThriftSuperColumnImpl<C2>(colSer.fromBytes(sc.getName()), sc);
        }
        else if (column.isSetCounter_super_column()) {
            CounterSuperColumn sc = column.getCounter_super_column();
            return new ThriftCounterSuperColumnImpl<C2>(colSer.fromBytes(sc.getName()), sc);
        }
        throw new UnsupportedOperationException("\'" + columnName + "\' is not a composite column");
    }

    @Override
    public <C2> Column<C2> getSuperColumn(int idx, Serializer<C2> colSer) {
        ColumnOrSuperColumn column = this.columns.get(idx);
        if (column == null) {
            // TODO: throw an exception
            return null;
        }
        else if (column.isSetSuper_column()) {
            SuperColumn sc = column.getSuper_column();
            return new ThriftSuperColumnImpl<C2>(colSer.fromBytes(sc.getName()), sc);
        }
        else if (column.isSetCounter_super_column()) {
            CounterSuperColumn sc = column.getCounter_super_column();
            return new ThriftCounterSuperColumnImpl<C2>(colSer.fromBytes(sc.getName()), sc);
        }
        throw new UnsupportedOperationException("\'" + idx + "\' is not a super column");
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
        if (columns.isEmpty())
            return false;

        ColumnOrSuperColumn sosc = columns.get(0);
        return sosc.isSetSuper_column() || sosc.isSetCounter_super_column();
    }

    private ColumnOrSuperColumn getColumn(C columnName) {
        constructMap();
        return lookup.get(columnName);
    }
    
    private void constructMap() {
        if (lookup == null) {
            lookup = Maps.newHashMap();
            for (ColumnOrSuperColumn column : columns) {
                if (column.isSetSuper_column()) {
                    lookup.put(colSer.fromBytes(column.getSuper_column().getName()), column);
                }
                else if (column.isSetColumn()) {
                    lookup.put(colSer.fromBytes(column.getColumn().getName()), column);
                }
                else if (column.isSetCounter_column()) {
                    lookup.put(colSer.fromBytes(column.getCounter_column().getName()), column);
                }
                else if (column.isSetCounter_super_column()) {
                    lookup.put(colSer.fromBytes(column.getCounter_super_column().getName()), column);
                }
                else {
                    throw new UnsupportedOperationException("Unknown column type");
                }
            }
        }
    }

    @Override
    public Collection<C> getColumnNames() {
        constructMap();
        return lookup.keySet();
    }
}
