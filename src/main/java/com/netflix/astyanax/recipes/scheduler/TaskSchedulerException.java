package com.netflix.astyanax.recipes.scheduler;

public class TaskSchedulerException extends Exception {
    private static final long serialVersionUID = 3775821456620702083L;

    public TaskSchedulerException(String message) {
        super(message);
    }

    public TaskSchedulerException(Throwable t) {
        super(t);
    }

    public TaskSchedulerException(String message, Throwable cause) {
        super(message, cause);
    }

}
