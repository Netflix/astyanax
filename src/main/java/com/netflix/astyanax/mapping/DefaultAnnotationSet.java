package com.netflix.astyanax.mapping;

import java.lang.reflect.Field;

/**
 * The default annotation set. Supports {@link Id} and {@link Column}
 */
public class DefaultAnnotationSet implements AnnotationSet2<Id, Column> {

    private Integer defaultTtl = null;

    public DefaultAnnotationSet() {
        this(null);
    }

    /**
     * Specify a default time-to-live for columns with no own ttl specification.
     * 
     * @param defaultColumnTtl
     *            a negative value or 0 disables default ttl setting
     */
    public DefaultAnnotationSet(Integer defaultColumnTtl) {
        this.defaultTtl = defaultColumnTtl;
    }

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

    @Override
    public int getColumnTtl(Field field, Column annotation) {
        if (annotation.ttl() > -1) {
            return annotation.ttl();
        } else if (defaultTtl != null && defaultTtl.intValue() > 0) {
            return defaultTtl.intValue();
        }
        return -1;
    }
}
