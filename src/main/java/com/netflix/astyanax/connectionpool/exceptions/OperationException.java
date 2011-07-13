package com.netflix.astyanax.connectionpool.exceptions;

/**
 * Application exception for an operation executed within the context of the
 * connection pool.  An application exception varies from other ConnectionException
 * in that it will immediately roll up to the client and cannot fail over.
 * Examples of application exceptions are invalid request formats.
 * 
 * @author elandau
 *
 */
public class OperationException extends ConnectionException {
	public OperationException(String message) {
		super(message, false);
	}
	
	public OperationException(Throwable t) {
		super(t, false);
	}
	
	public OperationException(String message, Throwable cause) {
        super(message, cause, false);
    }

}
