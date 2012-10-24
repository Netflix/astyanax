package com.netflix.astyanax.consistency;

import com.netflix.astyanax.model.ConsistencyLevel;

public final class OneOneConsistencyLevelPolicy implements ConsistencyLevelPolicy {

    private OneOneConsistencyLevelPolicy() {
    }

    @Override
    public ConsistencyLevel getReadConsistencyLevel() {
        return ConsistencyLevel.CL_ONE;
    }

    @Override
    public ConsistencyLevel getWriteConsistencyLevel() {
        return ConsistencyLevel.CL_ONE;
    }

    private static class PolicyHolder {
        static final ConsistencyLevelPolicy policy = new OneOneConsistencyLevelPolicy();
    }

    public static ConsistencyLevelPolicy get() {
        return PolicyHolder.policy;
    }
}
