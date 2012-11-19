package com.netflix.astyanax.recipes.scheduler;

public class SchedulerException extends Exception {
    private static final long serialVersionUID = 3917437309288808628L;

    public SchedulerException(String message) {
        super(message);
    }

    public SchedulerException(Throwable t) {
        super(t);
    }

    public SchedulerException(String message, Throwable cause) {
        super(message, cause);
    }
}
