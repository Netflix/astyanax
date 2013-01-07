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
	 * fully qualified class name of custom Serializer
	 * @return
	 */
	Class<?> value();
}
