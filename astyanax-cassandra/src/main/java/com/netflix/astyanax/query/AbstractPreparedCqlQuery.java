/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.astyanax.query;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.DoubleSerializer;
import com.netflix.astyanax.serializers.FloatSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.ShortSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;

public abstract class AbstractPreparedCqlQuery<K, C> implements PreparedCqlQuery<K, C> {
    private List<ByteBuffer> values = Lists.newArrayList();

    protected List<ByteBuffer> getValues() {
        return values;
    }
    
    @Override
    public <V> PreparedCqlQuery<K, C> withByteBufferValue(V value, Serializer<V> serializer) {
        return withValue(serializer.toByteBuffer(value));
    }

    @Override
    public PreparedCqlQuery<K, C> withValue(ByteBuffer value) {
        values.add(value);
        return this;
    }

    @Override
    public PreparedCqlQuery<K, C> withValues(List<ByteBuffer> values) {
        this.values.addAll(values);
        return this;
    }

    @Override
    public PreparedCqlQuery<K, C> withStringValue(String value) {
        return withByteBufferValue(value, StringSerializer.get());
    }

    @Override
    public PreparedCqlQuery<K, C> withIntegerValue(Integer value) {
        return withByteBufferValue(value, IntegerSerializer.get());
    }

    @Override
    public PreparedCqlQuery<K, C> withBooleanValue(Boolean value) {
        return withByteBufferValue(value, BooleanSerializer.get());
    }

    @Override
    public PreparedCqlQuery<K, C> withDoubleValue(Double value) {
        return withByteBufferValue(value, DoubleSerializer.get());
    }

    @Override
    public PreparedCqlQuery<K, C> withLongValue(Long value) {
        return withByteBufferValue(value, LongSerializer.get());
    }

    @Override
    public PreparedCqlQuery<K, C> withFloatValue(Float value) {
        return withByteBufferValue(value, FloatSerializer.get());
    }

    @Override
    public PreparedCqlQuery<K, C> withShortValue(Short value) {
        return withByteBufferValue(value, ShortSerializer.get());
    }

    @Override
    public PreparedCqlQuery<K, C> withUUIDValue(UUID value) {
        return withByteBufferValue(value, UUIDSerializer.get());
    }

}
