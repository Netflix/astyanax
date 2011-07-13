package com.netflix.astyanax.connectionpool.exceptions;

public class TokenRangeOfflineException extends ConnectionException {
		
    public TokenRangeOfflineException(String message) {
        super(message, false);
    }

    public TokenRangeOfflineException(Throwable cause) {
        super(cause, false);
    }

    public TokenRangeOfflineException(String message, Throwable cause) {
        super(message, cause, false);
    }
}
