package com.netflix.astyanax.retry;

import com.netflix.astyanax.retry.RetryPolicy.RetryPolicyFactory;

public class RunOnceRetryPolicyFactory implements RetryPolicyFactory {

	@Override
	public RetryPolicy createRetryPolicy() {
		return new RunOnce();
	}
}
