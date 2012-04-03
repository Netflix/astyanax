package com.netflix.astyanax.connectionpool.exceptions;

public class OperationTimeoutException extends ConnectionException implements
        IsTimeoutException, IsRetryableException {
    /**
     * 
     */
    private static final long serialVersionUID = 5676170035940390111L;

    public OperationTimeoutException(String message) {
        super(message);
    }

    public OperationTimeoutException(Throwable t) {
        super(t);
    }

    public OperationTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
