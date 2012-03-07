package com.netflix.astyanax.connectionpool.exceptions;

public class TransactionException extends ConnectionException {
	private static final long serialVersionUID = -1556352199503400553L;

	public TransactionException(String message) {
        super(message);
    }

    public TransactionException(Throwable cause) {
        super(cause);
    }

    public TransactionException(String message, Throwable cause) {
        super(message, cause);
    }
}
