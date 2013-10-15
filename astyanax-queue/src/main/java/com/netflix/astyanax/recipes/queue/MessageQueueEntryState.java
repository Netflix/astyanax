package com.netflix.astyanax.recipes.queue;

enum MessageQueueEntryState {
    None,
    Waiting,
    Busy,
    Done,
    Acquired,
}