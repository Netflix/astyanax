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
package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import javax.persistence.Column;
import javax.persistence.OrderBy;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.serializers.PrefixedSerializer;
import com.netflix.astyanax.serializers.ByteBufferSerializer;

/**
 * Mapper from a field to a ByteBuffer
 * @author elandau
 *
 * @param <T>
 */
public class FieldMapper<T> {
    final Serializer<T>     serializer;
    final Field             field;
    final String            name;
    final boolean           reversed;

    enum Order {
        ASC,
        DESC,
    }
    
    public FieldMapper(final Field field) {
        this(field, null);
    }
    
    public FieldMapper(final Field field, ByteBuffer prefix) {
        
        if (prefix != null) {
            this.serializer   = new PrefixedSerializer<ByteBuffer, T>(prefix, ByteBufferSerializer.get(), (Serializer<T>) MappingUtils.getSerializerForField(field));
        }
        else {
            this.serializer       = (Serializer<T>) MappingUtils.getSerializerForField(field);
        }
        this.field            = field;
        
        Column columnAnnotation = field.getAnnotation(Column.class);
        if (columnAnnotation == null || columnAnnotation.name().isEmpty()) {
            name = field.getName();
        }
        else {
            name = columnAnnotation.name();
        }
        
        OrderBy orderByAnnotation = field.getAnnotation(OrderBy.class);
        if (orderByAnnotation == null) {
            reversed = false;
        }
        else {
            Order order = Order.valueOf(orderByAnnotation.value());
            reversed = (order == Order.DESC);
        }
    }

    public Serializer<?> getSerializer() {
        return serializer;
    }
    
    public ByteBuffer toByteBuffer(Object entity) throws IllegalArgumentException, IllegalAccessException {
        return serializer.toByteBuffer(getValue(entity));
    }
    
    public T fromByteBuffer(ByteBuffer buffer) {
        return serializer.fromByteBuffer(buffer);
    }
    
    public T getValue(Object entity) throws IllegalArgumentException, IllegalAccessException {
        return (T)field.get(entity);
    }
    
    public ByteBuffer valueToByteBuffer(Object value) {
        return serializer.toByteBuffer((T)value);
    }
    
    public void setValue(Object entity, Object value) throws IllegalArgumentException, IllegalAccessException {
        field.set(entity, value);
    }
    
    public void setField(Object entity, ByteBuffer buffer) throws IllegalArgumentException, IllegalAccessException {
        field.set(entity, fromByteBuffer(buffer));
    }

    public boolean isAscending() {
        return reversed == false;
    }
    
    public boolean isDescending() {
        return reversed == true;
    }
    
    public String getName() {
        return name;
    }
}
