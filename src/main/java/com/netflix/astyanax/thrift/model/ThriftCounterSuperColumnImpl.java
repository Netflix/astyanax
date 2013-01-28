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

import java.nio.ByteBuffer;

import org.apache.cassandra.thrift.CounterSuperColumn;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.AbstractColumnImpl;
import com.netflix.astyanax.model.ColumnList;

public class ThriftCounterSuperColumnImpl<C> extends AbstractColumnImpl<C> {
    private final CounterSuperColumn column;

    public ThriftCounterSuperColumnImpl(C name, CounterSuperColumn column) {
        super(name);
        this.column = column;
    }

    @Override
    public <V> V getValue(Serializer<V> valSer) {
        throw new UnsupportedOperationException("CounterSuperColumn \'" + getName() + "\' has no value");
    }

    @Override
    public <C2> ColumnList<C2> getSubColumns(Serializer<C2> ser) {
        return new ThriftCounterColumnListImpl<C2>(this.column.getColumns(), ser);
    }

    @Override
    public boolean isParentColumn() {
        return true;
    }

    @Override
    public long getTimestamp() {
        throw new UnsupportedOperationException("CounterSuperColumn \'" + getName() + "\' has no timestamp");
    }

    @Override
    public ByteBuffer getRawName() {
        return ByteBuffer.wrap(column.getName());
    }

    @Override
    public int getTtl() {
        throw new UnsupportedOperationException("CounterSuperColumn \'" + getName() + "\' has no ttl");
    }

    @Override
    public boolean hasValue() {
        return false;
    }
}
