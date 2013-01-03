package com.netflix.astyanax.recipes.queue;

enum MessageQueueEntryType {
//    InternalEvent,      // Internal event
    Metadata,
    Lock,                  // Lock column
    Message,               // Event in the queue
}