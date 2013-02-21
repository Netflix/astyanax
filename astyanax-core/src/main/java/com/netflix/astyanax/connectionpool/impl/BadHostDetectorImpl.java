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

import java.util.concurrent.LinkedBlockingQueue;

import com.netflix.astyanax.connectionpool.BadHostDetector;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;

/**
 * BadHostDetector which marks the host as failed if there is a transport
 * exception or if it timed out too many times within a certain time window
 * 
 * @author elandau
 *
 */
public class BadHostDetectorImpl implements BadHostDetector {
	
	private final LinkedBlockingQueue<Long> timeouts;
	private final ConnectionPoolConfiguration config;
	
	public BadHostDetectorImpl(ConnectionPoolConfiguration config) {
		this.timeouts = new LinkedBlockingQueue<Long>();
		this.config = config;
	}
	
	public String toString() {
        return new StringBuilder()
             .append("BadHostDetectorImpl[")
             .append("count=").append(config.getMaxTimeoutCount())
             .append(",window=").append(config.getTimeoutWindow())
             .append("]")
             .toString();
	}
	
	@Override
	public Instance createInstance() {
		return new Instance() {
			@Override
			public boolean addTimeoutSample() {
				long currentTimeMillis = System.currentTimeMillis();
				
				timeouts.add(currentTimeMillis);
				
				// Determine if the host exceeded timeoutCounter exceptions in
				// the timeoutWindow, in which case this is determined to be a
				// failure
				if (timeouts.size() > config.getMaxTimeoutCount()) {
					Long last = timeouts.remove();
					if ((currentTimeMillis - last.longValue()) < config.getTimeoutWindow()) {
						return true;
					}
				}
				return false;
			}
		};
	}

	@Override
	public void removeInstance(Instance instance) {
		// NOOP
	}
}
