package com.netflix.astyanax.consistency;

import com.netflix.astyanax.model.ConsistencyLevel;

public final class OneLQuorumConsistencyLevelPolicy implements ConsistencyLevelPolicy {

    private OneLQuorumConsistencyLevelPolicy() {
    }

    @Override
    public ConsistencyLevel getReadConsistencyLevel() {
        return ConsistencyLevel.CL_ONE;
    }

    @Override
    public ConsistencyLevel getWriteConsistencyLevel() {
        return ConsistencyLevel.CL_LOCAL_QUORUM;
    }

    private static class PolicyHolder {
        static final ConsistencyLevelPolicy policy = new OneLQuorumConsistencyLevelPolicy();
    }

    public static ConsistencyLevelPolicy get() {
        return PolicyHolder.policy;
    }

}
