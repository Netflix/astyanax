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
