package com.netflix.astyanax.connectionpool.exceptions;

public class AuthenticationException extends OperationException {
    /**
	 * 
	 */
    private static final long serialVersionUID = -1376061218661301537L;

    /**
     * 
     */

    public AuthenticationException(String message) {
        super(message);
    }

    public AuthenticationException(Throwable t) {
        super(t);
    }

    public AuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }
}
