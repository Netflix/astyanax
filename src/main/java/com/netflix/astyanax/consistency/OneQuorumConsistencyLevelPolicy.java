package com.netflix.astyanax.consistency;

import com.netflix.astyanax.model.ConsistencyLevel;

public final class OneQuorumConsistencyLevelPolicy implements ConsistencyLevelPolicy {

    private OneQuorumConsistencyLevelPolicy() {
    }

    @Override
    public ConsistencyLevel getReadConsistencyLevel() {
        return ConsistencyLevel.CL_ONE;
    }

    @Override
    public ConsistencyLevel getWriteConsistencyLevel() {
        return ConsistencyLevel.CL_QUORUM;
    }

    private static class PolicyHolder {
        static final ConsistencyLevelPolicy policy = new OneQuorumConsistencyLevelPolicy();
    }

    public static ConsistencyLevelPolicy get() {
        return PolicyHolder.policy;
    }

}
