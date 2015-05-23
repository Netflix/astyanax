package com.netflix.astyanax.cql.retrypolicies;

import com.netflix.astyanax.retry.RetryPolicy;

/**
 * Abstract base for all {@link RetryPolicy} implementation that want to use the retry policy from java driver.
 * @author poberai
 *
 */
public abstract class JavaDriverBasedRetryPolicy implements RetryPolicy {

	@Override
	public void begin() {
	}

	@Override
	public void success() {
	}

	@Override
	public void failure(Exception e) {
	}

	@Override
	public boolean allowRetry() {
		return false;
	}

	@Override
	public int getAttemptCount() {
		return 0;
	}

	@Override
	public RetryPolicy duplicate() {
		return null;
	}
	
	public abstract com.datastax.driver.core.policies.RetryPolicy getJDRetryPolicy();

}
