package com.netflix.astyanax.recipes.locks;

public class StaleLockException extends Exception {
    private static final long serialVersionUID = -1094508305645942319L;

    public StaleLockException(Exception e) {
        super(e);
    }

    public StaleLockException(String message, Exception e) {
        super(message, e);
    }

    public StaleLockException(String message) {
        super(message);
    }

}
