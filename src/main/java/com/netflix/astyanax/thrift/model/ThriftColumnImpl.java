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
import java.util.Date;
import java.util.UUID;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.AbstractColumnImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.ByteSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.DateSerializer;
import com.netflix.astyanax.serializers.DoubleSerializer;
import com.netflix.astyanax.serializers.FloatSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.ShortSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;

/**
 * 
 * 
 * TODO: All serializers
 * 
 * @author elandau
 * 
 * @param <C>
 */
public class ThriftColumnImpl<C> extends AbstractColumnImpl<C> {
    private final org.apache.cassandra.thrift.Column column;

    public ThriftColumnImpl(C name, org.apache.cassandra.thrift.Column column) {
        super(name);
        this.column = column;
    }

    @Override
    public <V> V getValue(Serializer<V> valSer) {
        return valSer.fromBytes(column.getValue());
    }

    @Override
    public <C2> ColumnList<C2> getSubColumns(Serializer<C2> ser) {
        throw new UnsupportedOperationException("SimpleColumn \'" + getName() + "\' has no children");
    }

    @Override
    public boolean isParentColumn() {
        return false;
    }

    @Override
    public long getTimestamp() {
        return column.getTimestamp();
    }

    @Override
    public ByteBuffer getRawName() {
        return ByteBuffer.wrap(column.getName());
    }
    
    @Override
    public int getTtl() {
        return column.isSetTtl() ? column.getTtl() : 0;
    }

    @Override
    public boolean hasValue() {
        return column.value != null && column.value.remaining() != 0;
    }
}
