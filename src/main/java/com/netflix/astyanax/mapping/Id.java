package com.netflix.astyanax.mapping;

import java.lang.annotation.*;

@Documented
@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
/**
 * This is a marker annotation for the field that should act as the
 * ID column
 */
public @interface Id {
    /**
     * The name by which this particular field should be persisted as. By
     * default, the name of the field is used
     * 
     * @return column name
     */
    String value() default "";
}
