package com.netflix.astyanax.recipes.scheduler;

enum SchedulerEntryState {
    None,
    Waiting,
    Busy,
    Done,
    Acquired,
}