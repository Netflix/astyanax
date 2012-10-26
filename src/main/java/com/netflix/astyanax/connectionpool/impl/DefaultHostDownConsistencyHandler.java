package com.netflix.astyanax.connectionpool.impl;

import com.netflix.astyanax.connectionpool.ConsistencyLevelPolicyHolder;
import com.netflix.astyanax.connectionpool.HostDownConsistencyHandler;

/**
 * The default implementation that just return the required consistency level policy
 *
 * @author Max Morozov
 */
public class DefaultHostDownConsistencyHandler implements HostDownConsistencyHandler {
    /**
     * Returns the consistency level policy that can be provided with the current
     *
     * @param obj         an object that should be processed and which consistency level is subject to change
     * @param partition   partition where the request will be executed
     */
    @Override
    public <CL> void handle(ConsistencyLevelPolicyHolder obj, HostConnectionPoolPartition<CL> partition) {
        //Do nothing
    }
}
