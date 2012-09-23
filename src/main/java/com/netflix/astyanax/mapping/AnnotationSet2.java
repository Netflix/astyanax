package com.netflix.astyanax.mapping;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

/**
 * Allows for any annotations to be used to mark columns in a bean
 */
public interface AnnotationSet2<ID extends Annotation, COLUMN extends Annotation>
        extends AnnotationSet<ID, COLUMN> {

    public int getColumnTtl(Field field, COLUMN annotation);
}
