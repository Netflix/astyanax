package com.netflix.astyanax.consistency;

import com.netflix.astyanax.model.ConsistencyLevel;

public final class OneTwoConsistencyLevelPolicy implements ConsistencyLevelPolicy {

    private OneTwoConsistencyLevelPolicy() {
    }

    @Override
    public ConsistencyLevel getReadConsistencyLevel() {
        return ConsistencyLevel.CL_ONE;
    }

    @Override
    public ConsistencyLevel getWriteConsistencyLevel() {
        return ConsistencyLevel.CL_TWO;
    }

    private static class PolicyHolder {
        static final ConsistencyLevelPolicy policy = new OneTwoConsistencyLevelPolicy();
    }

    public static ConsistencyLevelPolicy get() {
        return PolicyHolder.policy;
    }
}
