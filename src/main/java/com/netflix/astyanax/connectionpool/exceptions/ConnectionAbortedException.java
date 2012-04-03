package com.netflix.astyanax.connectionpool.exceptions;

public class ConnectionAbortedException extends ConnectionException implements
        IsRetryableException, IsDeadConnectionException {
    /**
     * 
     */
    private static final long serialVersionUID = 6918220226977765595L;

    public ConnectionAbortedException(String message) {
        super(message);
    }

    public ConnectionAbortedException(Throwable t) {
        super(t);
    }

    public ConnectionAbortedException(String message, Throwable cause) {
        super(message, cause);
    }

}
