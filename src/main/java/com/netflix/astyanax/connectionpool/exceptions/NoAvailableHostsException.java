package com.netflix.astyanax.connectionpool.exceptions;

public class NoAvailableHostsException extends ConnectionException {
	public NoAvailableHostsException(String message) {
		super(message, false);
	}
	
	public NoAvailableHostsException(Throwable t) {
		super(t, false);
	}
	
	public NoAvailableHostsException(String message, Throwable cause) {
        super(message, cause, false);
    }
}
