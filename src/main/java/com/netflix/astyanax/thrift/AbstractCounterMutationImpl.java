package com.netflix.astyanax.thrift;

import com.netflix.astyanax.CounterMutation;
import com.netflix.astyanax.model.ConsistencyLevel;

public abstract class AbstractCounterMutationImpl<K,C> implements CounterMutation<K,C> {

	protected ConsistencyLevel consistencyLevel;
	protected long timeout;
	
	public AbstractCounterMutationImpl(ConsistencyLevel consistencyLevel, long timeout) {
		this.consistencyLevel = consistencyLevel;
		this.timeout = timeout;
	}
	
	@Override
	public CounterMutation<K, C> setConsistencyLevel(ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
		return this;
	}

	@Override
	public CounterMutation<K, C> setTimeout(long timeout) {
		this.timeout = timeout;
		return this;
	}

}
