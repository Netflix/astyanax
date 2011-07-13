package com.netflix.astyanax.connectionpool.exceptions;

/**
 * No more connections may be opened on a host and no timeout was specified.
 * 
 * @author elandau
 *
 */
public class MaxConnsPerHostReachedException extends ConnectionException {
	public MaxConnsPerHostReachedException(String message) {
		super(message, true);
	}
	
	public MaxConnsPerHostReachedException(Throwable t) {
		super(t, true);
	}
	
	public MaxConnsPerHostReachedException(String message, Throwable cause) {
        super(message, cause, true);
    }
}
