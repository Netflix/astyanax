package com.netflix.astyanax.recipes.queue.entity;

public enum MessageQueueEntryState {
    None,
    Waiting,
    Busy,
    Done,
    Acquired,
}