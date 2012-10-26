package com.netflix.astyanax.connectionpool;

import com.netflix.astyanax.consistency.ConsistencyLevelPolicy;

/**
 * @author Max Morozov
 */
public interface ConsistencyLevelPolicyHolder {
    /**
     * Gets the consistency level policy of this operation.
     *
     * @return an object that keeps consistency levels for read and write operations
     */
    ConsistencyLevelPolicy getConsistencyLevelPolicy();

    /**
     * Set consistency level policy for this operation.
     *
     * @param consistencyLevelPolicy object that keeps consistency levels for read and write operations
     */
    void setConsistencyLevelPolicy(ConsistencyLevelPolicy consistencyLevelPolicy);
}
