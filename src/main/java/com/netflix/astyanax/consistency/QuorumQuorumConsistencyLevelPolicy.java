package com.netflix.astyanax.consistency;

import com.netflix.astyanax.model.ConsistencyLevel;

public final class QuorumQuorumConsistencyLevelPolicy implements ConsistencyLevelPolicy {

    private QuorumQuorumConsistencyLevelPolicy() {
    }

    @Override
    public ConsistencyLevel getReadConsistencyLevel() {
        return ConsistencyLevel.CL_QUORUM;
    }

    @Override
    public ConsistencyLevel getWriteConsistencyLevel() {
        return ConsistencyLevel.CL_QUORUM;
    }

    private static class PolicyHolder {
        static final ConsistencyLevelPolicy policy = new QuorumQuorumConsistencyLevelPolicy();
    }

    public static ConsistencyLevelPolicy get() {
        return PolicyHolder.policy;
    }
}
