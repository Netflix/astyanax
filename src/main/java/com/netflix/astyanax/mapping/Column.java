package com.netflix.astyanax.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    /**
     * The name by which this particular field should be persisted as. By
     * default, the name of the field is used
     * 
     * @return column name
     */
    String value() default "";

    /**
     * The time-to-live in seconds with which this particular field should be
     * persisted as. By default no expiration deadline (ttl) is set. Setting a
     * value of <code>0</code> disables expiration for this column, overriding a
     * class-wide setting on {@link DefaultAnnotationSet}.
     * 
     * @return
     */
    int ttl() default -1;
}
