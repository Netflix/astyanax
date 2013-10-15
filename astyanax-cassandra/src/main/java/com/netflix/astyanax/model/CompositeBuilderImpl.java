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
package com.netflix.astyanax.model;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.AbstractComposite.ComponentEquality;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.TimeUUIDSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class CompositeBuilderImpl implements CompositeBuilder {
    private AbstractComposite composite;
    private ComponentEquality equality = ComponentEquality.EQUAL;

    public CompositeBuilderImpl(AbstractComposite composite) {
        this.composite = composite;
    }

    @Override
    public CompositeBuilder addString(String value) {
        composite.addComponent(value, StringSerializer.get(), equality);
        return this;
    }

    @Override
    public CompositeBuilder addLong(Long value) {
        composite.addComponent(value, LongSerializer.get(), equality);
        return this;
    }

    @Override
    public CompositeBuilder addInteger(Integer value) {
        composite.addComponent(value, IntegerSerializer.get(), equality);
        return this;
    }

    @Override
    public CompositeBuilder addBoolean(Boolean value) {
        composite.addComponent(value, BooleanSerializer.get(), equality);
        return this;
    }

    @Override
    public CompositeBuilder addUUID(UUID value) {
        composite.addComponent(value, UUIDSerializer.get(), equality);
        return this;
    }

    @Override
    public <T> CompositeBuilder add(T value, Serializer<T> serializer) {
        composite.addComponent(value, serializer, equality);
        return this;
    }

    @Override
    public CompositeBuilder addTimeUUID(UUID value) {
        composite.addComponent(value, TimeUUIDSerializer.get(), equality);
        return this;
    }

    @Override
    public CompositeBuilder addTimeUUID(Long value, TimeUnit units) {
        composite.addComponent(TimeUUIDUtils.getMicrosTimeUUID(TimeUnit.MICROSECONDS.convert(value, units)),
                TimeUUIDSerializer.get(), equality);
        return this;
    }

    @Override
    public CompositeBuilder addBytes(byte[] bytes) {
        composite.addComponent(ByteBuffer.wrap(bytes), ByteBufferSerializer.get(), equality);
        return this;
    }

    @Override
    public CompositeBuilder addBytes(ByteBuffer bb) {
        composite.addComponent(bb, ByteBufferSerializer.get(), equality);
        return this;
    }

    @Override
    public ByteBuffer build() {
        return composite.serialize();
    }

    @Override
    public CompositeBuilder greaterThanEquals() {
        equality = ComponentEquality.GREATER_THAN_EQUAL;
        return this;
    }

    @Override
    public CompositeBuilder lessThanEquals() {
        equality = ComponentEquality.LESS_THAN_EQUAL;
        return this;
    }
}
