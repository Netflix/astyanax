package com.netflix.astyanax.recipes.queue;

public class DuplicateMessageException extends Exception {
    private static final long serialVersionUID = 3917437309288808628L;

    public DuplicateMessageException(String message) {
        super(message);
    }

    public DuplicateMessageException(Throwable t) {
        super(t);
    }

    public DuplicateMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
