package com.netflix.astyanax.connectionpool.exceptions;

public class SchemaDisagreementException extends OperationException {
    /**
     *
     */
    private static final long serialVersionUID = 8769688825913183141L;

    public SchemaDisagreementException(Throwable t) {
        super(t);
    }
}
