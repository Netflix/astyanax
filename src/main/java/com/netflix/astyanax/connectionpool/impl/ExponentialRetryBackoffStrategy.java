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

import java.util.Random;

import com.netflix.astyanax.connectionpool.RetryBackoffStrategy;

public class ExponentialRetryBackoffStrategy implements RetryBackoffStrategy{
	private final int max;
	private final long timeslot;
	private final long suspendTime;
	
	public ExponentialRetryBackoffStrategy(int max, int timeslot, int suspendTime) {
		this.max = (int) Math.pow(2, max);
		this.timeslot = timeslot;
		this.suspendTime = suspendTime;
	}
	
	@Override
	public Instance createInstance() {
		return new RetryBackoffStrategy.Instance() {
			private int c = 1;
			private boolean isSuspended = false;
			
			@Override
			public long nextDelay() {
				if (isSuspended) {
					isSuspended = false;
					return suspendTime;
				}
				
				c *= 2;
				if (c > max) 
					c = max;
				
				return new Random().nextInt(c) * timeslot;
			}

			@Override
			public void suspend() {
				isSuspended = true;
			}
		};
	}
}
