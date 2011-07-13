package com.netflix.astyanax.connectionpool.exceptions;

public class BadRequestException extends OperationException {
	public BadRequestException(String message) {
		super(message);
	}
	
	public BadRequestException(Throwable t) {
		super(t);
	}

	public BadRequestException(String message, Throwable cause) {
        super(message, cause);
    }
}