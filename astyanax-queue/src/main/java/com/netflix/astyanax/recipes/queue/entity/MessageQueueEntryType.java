package com.netflix.astyanax.recipes.queue.entity;

public enum MessageQueueEntryType {
    Lock,        // Lock column
    Message,     // Event in the queue
    Finalized,   // Indicates that the shard is locked and no more events will be added to it
}