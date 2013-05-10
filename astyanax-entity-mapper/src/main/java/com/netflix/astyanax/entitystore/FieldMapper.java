package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import com.netflix.astyanax.Serializer;

/**
 * Mapper from a field to a ByteBuffer
 * @author elandau
 *
 * @param <T>
 */
public class FieldMapper<T> {
    final Serializer<T>     serializer;
    final Field             field;

    public FieldMapper(final Field field) {
        this.serializer       = (Serializer<T>) MappingUtils.getSerializerForField(field);
        this.field            = field;
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
    
    public void setValue(Object entity, Object value) throws IllegalArgumentException, IllegalAccessException {
        field.set(entity, value);
    }
    
    public void setField(Object entity, ByteBuffer buffer) throws IllegalArgumentException, IllegalAccessException {
        field.set(entity, fromByteBuffer(buffer));
    }
}
