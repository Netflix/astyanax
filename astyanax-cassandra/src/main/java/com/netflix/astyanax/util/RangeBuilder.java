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
package com.netflix.astyanax.util;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import com.google.common.base.Preconditions;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.ByteBufferRange;
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
 * Utility builder to construct a ByteBufferRange to be used in a slice query.
 * 
 * @author elandau
 * 
 */
public class RangeBuilder {
    private static ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
    private static final int DEFAULT_MAX_SIZE = Integer.MAX_VALUE;

    private ByteBuffer start = EMPTY_BUFFER;
    private ByteBuffer end = EMPTY_BUFFER;
    private int limit = DEFAULT_MAX_SIZE;
    private boolean reversed = false;

    /**
     * @deprecated use setLimit instead
     */
    @Deprecated
    public RangeBuilder setMaxSize(int count) {
        return setLimit(count);
    }

    public RangeBuilder setLimit(int count) {
        Preconditions.checkArgument(count >= 0, "Invalid count in RangeBuilder : " + count);
        this.limit = count;
        return this;
    }

    /**
     * @deprecated Use setReversed(boolean reversed)
     * @return
     */
    @Deprecated
    public RangeBuilder setReversed() {
        reversed = true;
        return this;
    }

    public RangeBuilder setReversed(boolean reversed) {
        this.reversed = reversed;
        return this;
    }

    public RangeBuilder setStart(String value) {
        start = StringSerializer.get().toByteBuffer(value);
        return this;
    }

    public RangeBuilder setStart(byte[] value) {
        start = BytesArraySerializer.get().toByteBuffer(value);
        return this;
    }

    public RangeBuilder setStart(int value) {
        start = IntegerSerializer.get().toByteBuffer(value);
        return this;
    }

    public RangeBuilder setStart(long value) {
        start = LongSerializer.get().toByteBuffer(value);
        return this;
    }

    public RangeBuilder setStart(boolean value) {
        start = BooleanSerializer.get().toByteBuffer(value);
        return this;
    }

    public RangeBuilder setStart(ByteBuffer value) {
        start = ByteBufferSerializer.get().toByteBuffer(value);
        return this;
    }

    public RangeBuilder setStart(Date value) {
        start = DateSerializer.get().toByteBuffer(value);
        return this;
    }

    public RangeBuilder setStart(double value) {
        start = DoubleSerializer.get().toByteBuffer(value);
        return this;
    }

    public RangeBuilder setStart(UUID value) {
        start = UUIDSerializer.get().toByteBuffer(value);
        return this;
    }

    public <T> RangeBuilder setStart(T value, Serializer<T> serializer) {
        start = serializer.toByteBuffer(value);
        return this;
    }

    public RangeBuilder setEnd(String value) {
        end = StringSerializer.get().toByteBuffer(value);
        return this;
    }

    public RangeBuilder setEnd(byte[] value) {
        end = BytesArraySerializer.get().toByteBuffer(value);
        return this;
    }

    public RangeBuilder setEnd(int value) {
        end = IntegerSerializer.get().toByteBuffer(value);
        return this;
    }

    public RangeBuilder setEnd(long value) {
        end = LongSerializer.get().toByteBuffer(value);
        return this;
    }

    public RangeBuilder setEnd(boolean value) {
        end = BooleanSerializer.get().toByteBuffer(value);
        return this;
    }

    public RangeBuilder setEnd(ByteBuffer value) {
        end = ByteBufferSerializer.get().toByteBuffer(value);
        return this;
    }

    public RangeBuilder setEnd(Date value) {
        end = DateSerializer.get().toByteBuffer(value);
        return this;
    }

    public RangeBuilder setEnd(double value) {
        end = DoubleSerializer.get().toByteBuffer(value);
        return this;
    }

    public RangeBuilder setEnd(UUID value) {
        end = UUIDSerializer.get().toByteBuffer(value);
        return this;
    }

    public <T> RangeBuilder setEnd(T value, Serializer<T> serializer) {
        end = serializer.toByteBuffer(value);
        return this;
    }

    public ByteBufferRange build() {
        return new ByteBufferRangeImpl(clone(start), clone(end), limit, reversed);
    }

    public static ByteBuffer clone(ByteBuffer original) {
        ByteBuffer clone = ByteBuffer.allocate(original.capacity());
        original.rewind();// copy from the beginning
        clone.put(original);
        original.rewind();
        clone.flip();
        return clone;
    }
}
