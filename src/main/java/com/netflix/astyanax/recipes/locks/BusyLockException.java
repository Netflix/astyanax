package com.netflix.astyanax.recipes.locks;

public class BusyLockException extends Exception {
    private static final long serialVersionUID = -6818914810045830278L;

    public BusyLockException(Exception e) {
        super(e);
    }

    public BusyLockException(String message, Exception e) {
        super(message, e);
    }

    public BusyLockException(String message) {
        super(message);
    }
}
