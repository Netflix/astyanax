package com.netflix.astyanax.recipes.queue;

public class MessageQueueException extends Exception {
    private static final long serialVersionUID = 3917437309288808628L;

    public MessageQueueException(String message) {
        super(message);
    }

    public MessageQueueException(Throwable t) {
        super(t);
    }

    public MessageQueueException(String message, Throwable cause) {
        super(message, cause);
    }
}
