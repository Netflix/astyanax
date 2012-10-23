package com.netflix.astyanax.shallows;

import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationFilterFactory;

public class EmptyOperationFilterFactory implements OperationFilterFactory {
    private final static OperationFilterFactory instance = new EmptyOperationFilterFactory();
    
    public static OperationFilterFactory getInstance() {
        return instance;
    }
            
    @Override
    public <R, CL> Operation<R, CL> attachFilter(Operation<R, CL> operation) {
        return operation;
    }
}
