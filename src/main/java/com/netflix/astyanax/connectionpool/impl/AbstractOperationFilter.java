package com.netflix.astyanax.connectionpool.impl;

import java.nio.ByteBuffer;

import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

public class AbstractOperationFilter<R, CL> implements Operation<R, CL>{

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

}
