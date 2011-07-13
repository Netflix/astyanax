package com.netflix.astyanax.connectionpool.exceptions;

public class PoolTimeoutException extends ConnectionException {
	public PoolTimeoutException(String message) {
		super(message, true);
	}
	
	public PoolTimeoutException(Throwable t) {
		super(t, true);
	}
	
	public PoolTimeoutException(String message, Throwable cause) {
        super(message, cause, true);
    }
}
