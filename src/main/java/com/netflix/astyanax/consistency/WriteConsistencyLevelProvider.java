package com.netflix.astyanax.consistency;

import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * @author Max Morozov
 */
public interface WriteConsistencyLevelProvider {
    /**
     * Gets write consistency level of this operation.
     *
     * @return operation's write consistency level
     */
    ConsistencyLevel getWriteConsistencyLevel();

    /**
     * Set write consistency level of this operation.
     *
     * @param consistencyLevel new write consistency level
     */
    void setWriteConsistencyLevel(ConsistencyLevel consistencyLevel);
}
