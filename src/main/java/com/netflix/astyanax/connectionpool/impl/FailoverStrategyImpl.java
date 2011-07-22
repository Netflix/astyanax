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
package com.netflix.astyanax.connectionpool.impl;

import com.netflix.astyanax.connectionpool.FailoverStrategy;

public class FailoverStrategyImpl implements FailoverStrategy {
	private final int maxRetries;
	private final int waitTime;
	
	public FailoverStrategyImpl(int maxRetries, int waitTime) {
		this.maxRetries = maxRetries;
		this.waitTime = waitTime;
	}
	
	@Override
	public int getMaxRetries() {
		return this.maxRetries;
	}
	
	@Override
	public int getWaitTime() {
		return this.waitTime;
	}

}
