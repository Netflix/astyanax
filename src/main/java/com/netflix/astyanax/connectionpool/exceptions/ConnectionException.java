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
package com.netflix.astyanax.connectionpool.exceptions;

/**
 * Connection exception caused by an error in the connection pool or a transport
 * error related to the connection itself.  Application errors are derived from
 * OperationException.
 * 
 * @author elandau
 *
 */
public abstract class ConnectionException extends Exception {
	private boolean retryable = false;
	
	public ConnectionException(String message, boolean retryable) {
		super(message);
		this.retryable = retryable;
	}
	
	public ConnectionException(Throwable t, boolean retryable) {
		super(t);
		this.retryable = retryable;
	}
	
	public ConnectionException(String message, Throwable cause, boolean retryable) {
        super(message, cause);
		this.retryable = retryable;
    }
	
	/**
	 * Determine if this type of exception is retryable from within the context
	 * of the entire connection pool.  For example, if one host is down then 
	 * the connection pool can try another host.  However, if the request is
	 * illegal retrying will always result in the same exception.
	 * @return
	 */
	public boolean isRetryable() {
		return this.retryable;
	}
}
