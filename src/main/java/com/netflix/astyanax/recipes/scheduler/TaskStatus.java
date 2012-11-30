package com.netflix.astyanax.recipes.scheduler;

public enum TaskStatus {
    WAITING,
    RUNNING,
    DONE,
    FAILED, 
    SKIPPED
}
