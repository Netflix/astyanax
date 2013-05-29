package com.netflix.astyanax.recipes.queue.entity;

public enum MessageMetadataEntryType {
    Lock,
    Unique,
    MessageId,
    Field
}
