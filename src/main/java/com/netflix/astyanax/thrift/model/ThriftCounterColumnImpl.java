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
import com.netflix.astyanax.serializers.LongSerializer;

public class ThriftCounterColumnImpl<C> implements Column<C> {

    private final C name;
    private final org.apache.cassandra.thrift.CounterColumn column;

    public ThriftCounterColumnImpl(C name, org.apache.cassandra.thrift.CounterColumn column) {
        this.name = name;
        this.column = column;
    }

    @Override
    public C getName() {
        return this.name;
    }

    @Override
    public <V> V getValue(Serializer<V> valSer) {
        throw new UnsupportedOperationException("CounterColumn \'" + this.name
                + "\' has no generic value. Call getLongValue().");
    }

    @Override
    public String getStringValue() {
        throw new UnsupportedOperationException("CounterColumn \'" + this.name
                + "\' has no String value. Call getLongValue().");
    }

    @Override
    public byte getByteValue() {
        throw new UnsupportedOperationException("CounterColumn \'" + this.name
                + "\' has no Byte value. Call getLongValue().");
    }
    
    @Override
    public short getShortValue() {
        throw new UnsupportedOperationException("CounterColumn \'" + this.name
                + "\' has no Short value. Call getLongValue().");
    }
    
    @Override
    public int getIntegerValue() {
        throw new UnsupportedOperationException("CounterColumn \'" + this.name
                + "\' has no Integer value. Call getLongValue().");
    }

    @Override
    public long getLongValue() {
        return this.column.getValue();
    }

    @Override
    public <C2> ColumnList<C2> getSubColumns(Serializer<C2> ser) {
        throw new UnsupportedOperationException("CounterColumn \'" + this.name
                + "\' has no sub columns. Call getLongValue().");
    }

    @Override
    public boolean isParentColumn() {
        return false;
    }

    @Override
    public byte[] getByteArrayValue() {
        throw new UnsupportedOperationException("CounterColumn \'" + this.name
                + "\' has no byte[] value. Call getLongValue().");
    }

    @Override
    public boolean getBooleanValue() {
        throw new UnsupportedOperationException("CounterColumn \'" + this.name
                + "\' has no Boolean value. Call getLongValue().");
    }

    @Override
    public ByteBuffer getByteBufferValue() {
        return LongSerializer.get().toByteBuffer(column.getValue());
    }

    @Override
    public Date getDateValue() {
        throw new UnsupportedOperationException("CounterColumn \'" + this.name
                + "\' has no Date value. Call getLongValue().");
    }

    @Override
    public UUID getUUIDValue() {
        throw new UnsupportedOperationException("CounterColumn \'" + this.name
                + "\' has no UUID value. Call getLongValue().");
    }

    @Override
    public float getFloatValue() {
        throw new UnsupportedOperationException("CounterColumn \'" + this.name
                + "\' has no Float value. Call getLongValue().");
    }
    
    @Override
    public double getDoubleValue() {
        throw new UnsupportedOperationException("CounterColumn \'" + this.name
                + "\' has no Double value. Call getLongValue().");
    }

    @Override
    public long getTimestamp() {
        throw new UnsupportedOperationException("CounterColumn \'" + this.name + "\' has no timestamp");
    }

    @Override
    public ByteBuffer getRawName() {
        return ByteBuffer.wrap(column.getName());
    }

    @Override
    public int getTtl() {
        return 0;
    }

    @Override
    public boolean hasValue() {
        return true;
    }

}
