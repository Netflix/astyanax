package com.netflix.astyanax;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ConsistencyLevel;

public interface CounterMutation<K, C> {
	OperationResult<Void> execute() throws ConnectionException;
	
	/**
	 * Set the consistency level for this query
	 * @param consistencyLevel
	 */
	CounterMutation<K,C> setConsistencyLevel(ConsistencyLevel consistencyLevel);
	
	/**
	 * Set the timeout for this operation.  
	 * @param timeout	In milliseconds
	 */
	CounterMutation<K,C> setTimeout(long timeout);
	
}
