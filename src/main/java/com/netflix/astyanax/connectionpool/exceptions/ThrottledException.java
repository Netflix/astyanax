package com.netflix.astyanax.connectionpool.exceptions;

public class ThrottledException extends ConnectionException implements
        IsRetryableException {
    private static final long serialVersionUID = 1257641642867458438L;

    public ThrottledException(String message) {
        super(message);
    }

    public ThrottledException(Throwable t) {
        super(t);
    }

    public ThrottledException(String message, Throwable cause) {
        super(message, cause);
    }
}
