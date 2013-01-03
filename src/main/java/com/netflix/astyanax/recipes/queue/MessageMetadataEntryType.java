package com.netflix.astyanax.recipes.queue;

public enum MessageMetadataEntryType {
    Lock,
    Unique,
    MessageId,
    Field
}
