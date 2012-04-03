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
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.DateSerializer;
import com.netflix.astyanax.serializers.DoubleSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
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
public class ThriftColumnImpl<C> implements Column<C> {
    private final C name;
    private final org.apache.cassandra.thrift.Column column;

    public ThriftColumnImpl(C name, org.apache.cassandra.thrift.Column column) {
        this.name = name;
        this.column = column;
    }

    @Override
    public C getName() {
        return name;
    }

    @Override
    public <V> V getValue(Serializer<V> valSer) {
        return valSer.fromBytes(column.getValue());
    }

    @Override
    public String getStringValue() {
        return StringSerializer.get().fromBytes(column.getValue());
    }

    @Override
    public int getIntegerValue() {
        return IntegerSerializer.get().fromBytes(column.getValue());
    }

    @Override
    public long getLongValue() {
        return LongSerializer.get().fromBytes(column.getValue());
    }

    @Override
    public <C2> ColumnList<C2> getSubColumns(Serializer<C2> ser) {

        throw new UnsupportedOperationException("SimpleColumn \'" + name
                + "\' has no children");
    }

    @Override
    public boolean isParentColumn() {
        return false;
    }

    @Override
    public byte[] getByteArrayValue() {
        return BytesArraySerializer.get().fromBytes(column.getValue());
    }

    @Override
    public boolean getBooleanValue() {
        return BooleanSerializer.get().fromBytes(column.getValue());
    }

    @Override
    public ByteBuffer getByteBufferValue() {
        return ByteBufferSerializer.get().fromBytes(column.getValue());
    }

    @Override
    public Date getDateValue() {
        return DateSerializer.get().fromBytes(column.getValue());
    }

    @Override
    public UUID getUUIDValue() {
        return UUIDSerializer.get().fromBytes(column.getValue());
    }

    @Override
    public double getDoubleValue() {
        return DoubleSerializer.get().fromBytes(column.getValue());
    }

    @Override
    public long getTimestamp() {
        return column.getTimestamp();
    }

    @Override
    public ByteBuffer getRawName() {
        return ByteBuffer.wrap(column.getName());
    }

}
