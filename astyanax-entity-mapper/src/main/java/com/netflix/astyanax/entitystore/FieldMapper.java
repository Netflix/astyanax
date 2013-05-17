package com.netflix.astyanax.entitystore;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import javax.persistence.Column;

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
    final String            name;

    public FieldMapper(final Field field) {
        this.serializer       = (Serializer<T>) MappingUtils.getSerializerForField(field);
        this.field            = field;
        
        Column columnAnnotation = field.getAnnotation(Column.class);
        if (columnAnnotation == null || columnAnnotation.name().isEmpty()) {
            name = field.getName();
        }
        else {
            name = columnAnnotation.name();
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

    public String getName() {
        return name;
    }
}
