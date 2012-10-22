package com.netflix.astyanax.connectionpool.exceptions;

public class InterruptedOperationException extends ConnectionException {

    /**
     * 
     */
    private static final long serialVersionUID = -1983353895015277466L;

    public InterruptedOperationException(String message) {
        super(message);
    }

    public InterruptedOperationException(Throwable t) {
        super(t);
    }

    public InterruptedOperationException(String message, Throwable cause) {
        super(message, cause);
    }

}
