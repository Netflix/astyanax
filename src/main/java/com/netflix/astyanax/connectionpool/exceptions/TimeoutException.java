package com.netflix.astyanax.connectionpool.exceptions;

public class TimeoutException extends ConnectionException {
	public TimeoutException(String message) {
		super(message, true);
	}
	
	public TimeoutException(Throwable t) {
		super(t, true);
	}
	
	public TimeoutException(String message, Throwable cause) {
        super(message, cause, true);
    }
}
