package com.netflix.astyanax.recipes.queue;

public class KeyExistsException extends MessageQueueException {
    private static final long serialVersionUID = 3917437309288808628L;

    public KeyExistsException(String message) {
        super(message);
    }

    public KeyExistsException(Throwable t) {
        super(t);
    }

    public KeyExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
