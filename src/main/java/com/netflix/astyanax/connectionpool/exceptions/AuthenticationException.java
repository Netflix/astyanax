package com.netflix.astyanax.connectionpool.exceptions;

public class AuthenticationException extends Exception {

	private static final long serialVersionUID = 7034806133673052188L;

	public AuthenticationException(final String message, final Throwable cause) {
        super(message, cause);
    }
	
}
