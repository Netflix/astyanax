/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.thrift;

import com.netflix.astyanax.CounterMutation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ExecutionHelper;
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

    @Override
    public OperationResult<Void> execute() throws ConnectionException {
        return ExecutionHelper.blockingExecute(this);
    }
}
