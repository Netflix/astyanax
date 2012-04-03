package com.netflix.astyanax.connectionpool.exceptions;

public class ThriftStateException extends ConnectionException implements
        IsDeadConnectionException, IsRetryableException {
    /**
	 * 
	 */
    private static final long serialVersionUID = -7163779789960683466L;

    public ThriftStateException(String message) {
        super(message);
    }

    public ThriftStateException(Throwable t) {
        super(t);
    }

    public ThriftStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
