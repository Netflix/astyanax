package com.netflix.astyanax.recipes.queue.entity;

import java.util.UUID;

import javax.persistence.Column;

public class MessageHistoryEntry {
    @Column
    private UUID timestamp;
    
    @Column
    private MessageHistory history;
}
