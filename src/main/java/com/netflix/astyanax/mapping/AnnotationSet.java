package com.netflix.astyanax.mapping;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

/**
 * Allows for any annotations to be used to mark columns in a bean
 */
@Deprecated
public interface AnnotationSet<ID extends Annotation, COLUMN extends Annotation> {
    /**
     * @return the Annotation class that marks a bean field as being the ID/Key
     */
    public Class<ID> getIdAnnotation();

    /**
     * @return the Annotation class that marks a bean field as being
     *         persist-able.
     */
    public Class<COLUMN> getColumnAnnotation();

    /**
     * Return the ID/Key name to use
     * 
     * @param field
     *            the field from the bean
     * @param annotation
     *            the id annotation
     * @return name to use for the field (cannot be null)
     */
    public String getIdName(Field field, ID annotation);

    /**
     * Return the column name to use for the given field. NOTE: if the field
     * should not be persisted, return <code>null</code>.
     * 
     * @param field
     *            the field from the bean
     * @param annotation
     *            the column annotation
     * @return name to use for the field or null
     */
    public String getColumnName(Field field, COLUMN annotation);
}
