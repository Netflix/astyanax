package com.netflix.astyanax.entitystore;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface TTL {

	/**
	 * The time-to-live in seconds with which this particular field should be persisted as. 
	 * add a negative default value so that the same annotation can be used at method level
	 */
	int value() default -1;
}
