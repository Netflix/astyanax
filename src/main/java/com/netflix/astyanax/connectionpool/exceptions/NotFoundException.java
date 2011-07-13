package com.netflix.astyanax.connectionpool.exceptions;

public class NotFoundException extends OperationException {
	
    public NotFoundException(String message) {
        super(message);
    }

    public NotFoundException(Throwable cause) {
        super(cause);
    }

    public NotFoundException(String message, Throwable cause) {
        super(message, cause);
    }


}
