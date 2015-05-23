package com.netflix.astyanax.cql.retrypolicies;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision;
import com.netflix.astyanax.cql.ConsistencyLevelMapping;

/**
 * Class that encapsulates a RetryPolicy that can be used in a configurable way when doing reties. 
 * Users can choose whether to retry on just  reads / writes / unavailable exceptions or just all operations. 
 * Users can also decide to change the consistency level on different reties. 
 * 
 * @author poberai
 *
 */
public class ChangeConsistencyLevelRetryPolicy extends JavaDriverBasedRetryPolicy {

	// Policies for specific conditions
	private boolean retryOnAllConditions = true;
	private boolean retryOnReads = false;
	private boolean retryOnWrites = false;
	private boolean retryOnUnavailable = false;
	
	// The retry count
	private int retryCount = 0;
	// the next consistency level to use.
	private ConsistencyLevel nextConsistencyLevel;
	// throw when giving up or ignore failures
	private boolean suppressFinalFailure = false;

	public ChangeConsistencyLevelRetryPolicy() { 
	}
	
	public ChangeConsistencyLevelRetryPolicy retryOnAllConditions(boolean condition) {
		retryOnAllConditions = condition;
		return this;
	}
	
	public ChangeConsistencyLevelRetryPolicy retryOnReadTimeouts(boolean condition) {
		retryOnReads = condition;
		retryOnAllConditions = false;
		return this;
	}

	public ChangeConsistencyLevelRetryPolicy retryOnWriteTimeouts(boolean condition) {
		retryOnWrites = condition;
		retryOnAllConditions = false;
		return this;
	}

	public ChangeConsistencyLevelRetryPolicy retryOnUnavailable(boolean condition) {
		retryOnUnavailable = condition;
		retryOnAllConditions = false;
		return this;
	}
	
	public ChangeConsistencyLevelRetryPolicy withNumRetries(int retries) {
		retryCount = retries;
		return this;
	}

	public ChangeConsistencyLevelRetryPolicy withNextConsistencyLevel(com.netflix.astyanax.model.ConsistencyLevel cl) {
		nextConsistencyLevel = ConsistencyLevelMapping.getCL(cl);
		return this;
	}
	
	public ChangeConsistencyLevelRetryPolicy suppressFinalFailure(boolean condition) {
		suppressFinalFailure = condition;
		return this;
	}

	private com.datastax.driver.core.policies.RetryPolicy jdRetry = new com.datastax.driver.core.policies.RetryPolicy() {

		@Override
		public RetryDecision onReadTimeout(Statement query, ConsistencyLevel cl, 
										  int requiredResponses, int receivedResponses,
										  boolean dataRetrieved, int nbRetry) {
			
			boolean shouldRetry = retryOnAllConditions || retryOnReads;
			return checkRetry(query, cl, shouldRetry);
		}

		@Override
		public RetryDecision onWriteTimeout(Statement query, ConsistencyLevel cl,
											WriteType writeType, int requiredAcks, int receivedAcks,
											int nbRetry) {
			
			boolean shouldRetry = retryOnAllConditions || retryOnWrites;
			return checkRetry(query, cl, shouldRetry);
		}

		@Override
		public RetryDecision onUnavailable(Statement query, ConsistencyLevel cl,
										   int requiredReplica, int aliveReplica, int nbRetry) {

			boolean shouldRetry = retryOnAllConditions || retryOnUnavailable;
			return checkRetry(query, cl, shouldRetry);
		}
	};

	@Override
	public com.datastax.driver.core.policies.RetryPolicy getJDRetryPolicy() {
		return jdRetry;
	}

	private RetryDecision checkRetry(Statement query, ConsistencyLevel cl, boolean shouldRetry) {
		
		if (!shouldRetry || retryCount <= 0) {
			// We are out of retries. 
			if (suppressFinalFailure) {
				return RetryDecision.ignore();
			} else {
				return RetryDecision.rethrow();
			}
		}
		
		// Ok we should retry and have some tries left.
		retryCount--;    // Note this retry
		
		// Check if the consistency level needs to be changed
		if (nextConsistencyLevel != null) {
			return RetryDecision.retry(nextConsistencyLevel);
		} else {
			return RetryDecision.retry(cl);
		}
	}
}
