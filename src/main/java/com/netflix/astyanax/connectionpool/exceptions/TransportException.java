package com.netflix.astyanax.connectionpool.exceptions;

public class TransportException extends ConnectionException {
	public TransportException(String message) {
		super(message, true);
	}
	
	public TransportException(Throwable t) {
		super(t, true);
	}

	public TransportException(String message, Throwable cause) {
        super(message, cause, true);
    }
}
