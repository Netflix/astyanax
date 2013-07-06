package com.netflix.astyanax.connectionpool.impl;

import java.nio.ByteBuffer;

import com.netflix.astyanax.connectionpool.ConnectionContext;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * 
 * Class that wraps an {@link Operation} to provide extra functionality. It can be used by extending class to wrap operation executions
 * and then decorate the execute functionality with their own logic 
 * 
 * @author elandau
 *
 * @param <CL>
 * @param <R>
 */
public class AbstractOperationFilter<CL, R> implements Operation<CL, R>{

    private Operation<CL, R> next;
    
    public AbstractOperationFilter(Operation<CL, R> next) {
        this.next = next;
    }
    
    @Override
    public R execute(CL client, ConnectionContext state) throws ConnectionException {
        return next.execute(client, state);
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
