package com.netflix.astyanax.connectionpool.exceptions;

public class WalException extends ConnectionException {
    private static final long serialVersionUID = -1556352199503400553L;

    public WalException(String message) {
        super(message);
    }

    public WalException(Throwable cause) {
        super(cause);
    }

    public WalException(String message, Throwable cause) {
        super(message, cause);
    }
}
