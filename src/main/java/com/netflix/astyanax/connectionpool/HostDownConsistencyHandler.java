package com.netflix.astyanax.connectionpool;

import com.netflix.astyanax.connectionpool.impl.HostConnectionPoolPartition;

/**
 * Handles events from the pool manager about downed and recovered hosts, and
 * modifies operations' consistency level to improve availability
 *
 * @author Max Morozov
 */
public interface HostDownConsistencyHandler {
    /**
     * Returns the consistency level policy that can be provided with the current
     *
     * @param obj         an object that should be processed and which consistency level is subject to change
     * @param partition   partition where the request will be executed
     */
    <CL> void handle(ConsistencyLevelPolicyHolder obj, HostConnectionPoolPartition<CL> partition);
}
