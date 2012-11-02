package com.netflix.astyanax.consistency;

import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * @author Max Morozov
 */
public interface ReadConsistencyLevelProvider {
    /**
     * Gets read consistency level of this operation.
     *
     * @return operation's read consistency level
     */
    ConsistencyLevel getReadConsistencyLevel();

    /**
     * Set read consistency level of this operation.
     *
     * @param consistencyLevel new read consistency level
     */
    void setReadConsistencyLevel(ConsistencyLevel consistencyLevel);
}
