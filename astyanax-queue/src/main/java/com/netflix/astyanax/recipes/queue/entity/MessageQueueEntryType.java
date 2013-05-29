package com.netflix.astyanax.recipes.queue.entity;

public enum MessageQueueEntryType {
    Lock,                  // Lock column
    Message,               // Event in the queue
}