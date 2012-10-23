package com.netflix.astyanax.connectionpool.impl;

import java.util.List;

import com.google.common.collect.Lists;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationFilterFactory;

public class OperationFilterFactoryList implements OperationFilterFactory {

    private final List<OperationFilterFactory> filters = Lists.newArrayList();
    
    @Override
    public <R, CL> Operation<R, CL> attachFilter(Operation<R, CL> operation) {
        for (OperationFilterFactory factory : filters) {
            operation = factory.attachFilter(operation);
        }
        return operation;
    }
    
    public OperationFilterFactoryList addFilterFactory(OperationFilterFactory factory) {
        filters.add(factory);
        return this;
    }
}
