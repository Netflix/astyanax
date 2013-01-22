package com.netflix.astyanax.entitystore;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface TTL {

	/**
	 * The time-to-live in seconds with which this particular field should be persisted as. 
	 * By default no expiration deadline (ttl) is set.
	 * @return
	 */
	int value();
}
