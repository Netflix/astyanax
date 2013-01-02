package com.netflix.astyanax.recipes.queue;

public enum MessageStatus {
    WAITING,
    RUNNING,
    DONE,
    FAILED, 
    SKIPPED
}
