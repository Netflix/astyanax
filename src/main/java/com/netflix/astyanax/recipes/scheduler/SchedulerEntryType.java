package com.netflix.astyanax.recipes.scheduler;

enum SchedulerEntryType {
//    InternalEvent,      // Internal event
    Lock,               // Lock column
    Task,               // Event in the queue
    Metadata
}