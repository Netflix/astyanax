package com.netflix.astyanax.connectionpool.exceptions;

/**
 * No more connections may be opened on a host and no timeout was specified.
 * 
 * @author elandau
 * 
 */
public class HostDownException extends ConnectionException implements
        IsRetryableException, IsDeadConnectionException {
    /**
     * 
     */
    private static final long serialVersionUID = 6081587518856031437L;

    public HostDownException(String message) {
        super(message);
    }

    public HostDownException(Throwable t) {
        super(t);
    }

    public HostDownException(String message, Throwable cause) {
        super(message, cause);
    }
}
