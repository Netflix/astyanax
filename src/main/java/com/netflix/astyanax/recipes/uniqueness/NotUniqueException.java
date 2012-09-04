package com.netflix.astyanax.recipes.uniqueness;

public class NotUniqueException extends Exception {
    /**
     *
     */
    private static final long serialVersionUID = -3735805268823536495L;

    public NotUniqueException(Exception e) {
        super(e);
    }

    public NotUniqueException(String message, Exception e) {
        super(message, e);
    }

    public NotUniqueException(String message) {
        super(message);
    }
}
