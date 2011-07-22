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

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.netflix.astyanax.connectionpool.BadHostDetector;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.exceptions.TimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.TransportException;
import com.netflix.astyanax.connectionpool.exceptions.UnknownException;

/**
 * BadHostDetector which marks the host as failed if there is a transport
 * exception or if it timed out too many times within a certain time window
 * @author elandau
 *
 */
public class BadHostDetectorImpl implements BadHostDetector {
	
	private final NonBlockingHashMap<Host, LinkedBlockingQueue<Long>> errors;
	private final int timeoutCounter;
	private final int timeoutWindow;
	
	public BadHostDetectorImpl(int timeoutCounter, int timeoutWindow) {
		this.errors = new NonBlockingHashMap<Host, LinkedBlockingQueue<Long>>();
		this.timeoutCounter = timeoutCounter;
		this.timeoutWindow = timeoutWindow;
	}
	
	@Override
	public boolean checkFailure(Host host, ConnectionException e) {
		if (e instanceof TransportException || e instanceof UnknownException) {
			return true;
		}
		else if (e instanceof OperationException) {
			return false;
		}
		else if (e instanceof TimeoutException) {
			long currentTimeMillis = System.currentTimeMillis();
			
			errors.putIfAbsent(host, new LinkedBlockingQueue<Long>());
			errors.get(host).add(currentTimeMillis);
			
			// Determine if the host exceeded timeoutCounter exceptions in
			// the timeoutWindow, in which case this is determined to be a
			// failure
			if (errors.get(host).size() > timeoutCounter) {
				Long last = errors.get(host).remove();
				if (last.longValue() < (currentTimeMillis - timeoutWindow)) {
					return true;
				}
			}
		}
		return false;
	}
}
