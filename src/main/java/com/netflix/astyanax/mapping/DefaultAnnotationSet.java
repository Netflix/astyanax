package com.netflix.astyanax.mapping;

import java.lang.reflect.Field;

/**
 * The default annotation set. Supports {@link Id} and {@link Column}
 */
public class DefaultAnnotationSet implements AnnotationSet<Id, Column> {
    @Override
    public Class<Id> getIdAnnotation() {
        return Id.class;
    }

    @Override
    public Class<Column> getColumnAnnotation() {
        return Column.class;
    }

    @Override
    public String getIdName(Field field, Id annotation) {
        String name = annotation.value();
        return (name.length() > 0) ? name : field.getName();
    }

    @Override
    public String getColumnName(Field field, Column annotation) {
        String name = annotation.value();
        return (name.length() > 0) ? name : field.getName();
    }
}
