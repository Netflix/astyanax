package com.netflix.astyanax.consistency;

import com.netflix.astyanax.model.ConsistencyLevel;

public final class LQuorumLQuorumConsistencyLevelPolicy implements ConsistencyLevelPolicy {

    private LQuorumLQuorumConsistencyLevelPolicy() {
    }

    @Override
    public ConsistencyLevel getReadConsistencyLevel() {
        return ConsistencyLevel.CL_LOCAL_QUORUM;
    }

    @Override
    public ConsistencyLevel getWriteConsistencyLevel() {
        return ConsistencyLevel.CL_LOCAL_QUORUM;
    }

    private static class PolicyHolder {
        static final ConsistencyLevelPolicy policy = new LQuorumLQuorumConsistencyLevelPolicy();
    }

    public static ConsistencyLevelPolicy get() {
        return PolicyHolder.policy;
    }
}
