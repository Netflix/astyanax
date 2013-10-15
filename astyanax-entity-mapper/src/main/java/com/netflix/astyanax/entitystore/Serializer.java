package com.netflix.astyanax.entitystore;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface Serializer {
	/**
	 * Fully qualified class name of custom Serializer
	 */
	Class<?> value();
}
