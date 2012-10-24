package com.netflix.astyanax.consistency;

import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * The object that keeps read and write consistency levels together
 */
public interface ConsistencyLevelPolicy {
    ConsistencyLevel getReadConsistencyLevel();
    ConsistencyLevel getWriteConsistencyLevel();
}
