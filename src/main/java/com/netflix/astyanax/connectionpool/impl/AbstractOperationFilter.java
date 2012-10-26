package com.netflix.astyanax.connectionpool.impl;

import java.nio.ByteBuffer;

import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.consistency.ConsistencyLevelPolicy;
import com.netflix.astyanax.consistency.OneOneConsistencyLevelPolicy;

public class AbstractOperationFilter<R, CL> implements Operation<R, CL>{

    private ConsistencyLevelPolicy consistencyLevelPolicy = OneOneConsistencyLevelPolicy.get();
    private Operation<R, CL> next;
    
    public AbstractOperationFilter(Operation<R, CL> next) {
        this.next = next;
    }
    
    @Override
    public CL execute(R client) throws ConnectionException {
        return next.execute(client);
    }

    @Override
    public ByteBuffer getRowKey() {
        return next.getRowKey();
    }

    @Override
    public String getKeyspace() {
        return next.getKeyspace();
    }

    @Override
    public Host getPinnedHost() {
        return next.getPinnedHost();
    }

    /**
     * Gets the consistency level policy of this operation.
     *
     * @return an object that keeps consistency levels for read and write operations
     */
    @Override
    public ConsistencyLevelPolicy getConsistencyLevelPolicy() {
        return consistencyLevelPolicy;
    }

    /**
     * Set consistency level policy for this operation.
     *
     * @param consistencyLevelPolicy object that keeps consistency levels for read and write operations
     */
    @Override
    public void setConsistencyLevelPolicy(ConsistencyLevelPolicy consistencyLevelPolicy) {
        this.consistencyLevelPolicy = consistencyLevelPolicy;
    }
}
