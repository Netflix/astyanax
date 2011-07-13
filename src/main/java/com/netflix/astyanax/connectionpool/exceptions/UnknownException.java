package com.netflix.astyanax.connectionpool.exceptions;

public class UnknownException extends ConnectionException {
	public UnknownException(String message) {
		super(message, false);
	}
	
	public UnknownException(Throwable t) {
		super(t, false);
	}

	public UnknownException(String message, Throwable cause) {
        super(message, cause, false);
    }
}
